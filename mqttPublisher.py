# -*- coding: utf-8 -*-

from Queue import Queue
import threading
import logging
import time
import datetime
from pqueue import PersistentQueue
import paho.mqtt.client as mqtt
import json
from config import configuration
import socket
from sqlitedict import SqliteDict
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

class mqttPublisher(threading.Thread):
    def __init__(self, threadID, name, config, modbusRs2mqttPublisher,
                 gpsReader2mqttPublisher, tempReader2mqttPublisher):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.config = config
        self.log = logging.getLogger("mqttPublisher")
        self._mqttc = mqtt.Client(client_id=configuration["hostname"] + "_pub")
        self._mqttc.will_set("/" + self.config["customer"] + "/" + self.getCommonName() + "/status",
                             json.dumps({"v": "0"}), 2, False)
        self._mqttc.on_message = self.mqtt_on_message
        self._mqttc.on_connect = self.mqtt_on_connect
        self._mqttc.on_publish = self.mqtt_on_publish
        self._mqttc.on_subscribe = self.mqtt_on_subscribe
        self.failedPduCounter = 0
        self.txCounter = 0
        self.statistics = SqliteDict("/tmp/stats.sqlite")
        self.retransmitQueue = PersistentQueue("/tmp/retransmitQueue")
        #        self._mqttc.on_log = self.mqtt_on_log
        try:
            self._mqttc.tls_set(self.config["caFile"], self.config["certFile"], self.config["keyFile"])
        except IOError as e:
            self.log.critical("mqttPublisher cannot access cert or key (%s)" % e)
            raise e
        else:
            self.modbusRs2mqttPublisher = modbusRs2mqttPublisher
            self.gpsReader2mqttPublisher = gpsReader2mqttPublisher
            self.tempReader2mqttPublisher = tempReader2mqttPublisher
            self.tick = 0.01
            self.interval = 0.1
            self.exitFlag = False
            self.commonName = self.getCommonName()
        # self.modbusRs2mqttPublisher.put([{"v":"1","t":"status"}]) # rejestracja statusu online

    def mqtt_on_disconnect(self, mqttc, userdata, rc):
        self.log.debug("on_disconnect() called")

    def mqtt_on_log(self, mqttc, userdata, level, buf):
        # self.log.debug("mattSubscriber:mqtt_on_log(): " + buf)
        pass

    def mqtt_on_connect(self, mqttc, obj, flags, rc):
        self.log.debug("on_connect() called")
    #        self._mqttc.subscribe("/"+self.config["customer"] + "/"+self.getCommonName()+"/set/+", 0)
    #        self.log.debug("/"+self.config["customer"] + "/"+self.getCommonName()+"/set/+")
    #        self.log.debug("rc: " + str(rc))

    def mqtt_on_message(self, mosq, obj, msg):
        self.log.debug("mqttSubscriber: on_message() called")
        global message
        self.log.debug(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))
        message = msg.payload
        self.mqttSubscriber2modbusRs.put(msg)
        self.messageTime = int(time.time())

    def mqtt_on_publish(self, mosq, obj, mid):
        # self.log.debug("mid: " + str(mid))
        pass

    def mqtt_on_subscribe(self, mosq, obj, mid, granted_qos):
        self.log.debug("on_subscribed() called")
        self.log.debug("Subscribed: " + str(mid) + " " + str(granted_qos))

    def mqtt_on_log(self, mosq, obj, level, string):
        self.log.debug(string)

    def run(self):
        self.log.debug("Starting " + self.name)
        while True:
            self.mqttConnect()
            counter = 0
            while True:
                try:
                    r = self._mqttc.loop(timeout=0.1)
                    if r == mqtt.MQTT_ERR_CONN_LOST:
                        break
                except Exception as e:
                    self.log.debug(e)
                    self.log.debug("Connection broken")
                    break
                if counter * self.tick >= self.interval:
                    self.proccessPdu()
                    counter = 0
                if self.exitFlag:
                    break
                counter += 1
                time.sleep(self.tick)
            if self.exitFlag:
                break
        self.log.debug("Exiting " + self.name)

    def mqttConnect(self):
        self.log.debug("mqttConnect(): Trying to (re)connect")
        notConnected = True
        while notConnected:
            try:
                notConnected = self._mqttc.connect(self.config["host"], self.config["port"], self.config["keepAlive"])
            except socket.error as e:
                self.log.debug("mqttConnect(): " + str(e))
            except Exception as e:
                self.log.debug("mqttConnect(): Something bad has happened. " + str(e))
                self.log.error(e, exc_info=True)
                time.sleep(5)
        self.modbusRs2mqttPublisher.put([{"v": "1", "t": "status"}])  # rejestracja statusu online
        self.log.debug("mqttConnect(): (Re)connected")

    def stats(self, p):
        #self.log.debug("stats() called")
        #timestamp = p["timestamp"]
        #value = p["v"]
        pass

    def proccessPdu(self):
        # PDU FORMAT pdu = {"a":SLAVE ADDRESS,"r":REGISTER NAME,"v":REGISTER VALUE,"t": TYPE [sensor|status]}
        # modbusRS puts the array of PDUs on queue or other processes
        while not self.retransmitQueue.empty():
            self.log.debug("PURGING retransmit queue...")
            self.txCounter += 1
            p = self.retransmitQueue.get()
            self.stats(p)

            self.log.debug("TX counter: (%d)" % self.txCounter)
            if p["t"] == "sensor":
                topic = "/" + self.config["customer"] + "/" + self.commonName + "/" + str(p["a"]) + "/" + p[
                    "t"] + "/" + str(p["r"])
                payload = json.dumps({"v": p["v"], "timestamp": p["timestamp"]})
                self._mqttc.publish(topic, payload, qos=0)
            elif p["t"] == "status":
                topic = "/" + self.config["customer"] + "/" + self.commonName + "/status"
                payload = json.dumps({"v": p["v"], "timestamp": int(round(time.time() * 1000))})  # Nadaj timestamp
                self._mqttc.publish(topic, payload, qos=0)

        while not self.modbusRs2mqttPublisher.empty():
            pdus = self.modbusRs2mqttPublisher.get()
            for p in pdus:
                self.txCounter += 1
                self.stats(p)
                if p["t"] == "sensor":
                    topic = "/" + self.config["customer"] + "/" + self.commonName + "/" + str(p["a"]) + "/" + p[
                        "t"] + "/" + str(p["r"])
                    payload = json.dumps({"v": p["v"], "timestamp": p["timestamp"]})
                    retval = self._mqttc.publish(topic, payload, qos=0)
                    time.sleep(0.06)
                    self.log.debug("%s : %d" % (str(p["r"]), p["timestamp"]))
                    if retval[0] == mqtt.MQTT_ERR_NO_CONN:
                        self.log.warn("No connection to mqtt server. PDU not sent. (%s)" % p)
                        self.failedPduCounter += 1
                        self.log.debug("Total PDU failed (%d)" % self.failedPduCounter)
                        self.log.debug("Adding to retransmit queue...")
                        self.retransmitQueue.put(p)
                        return
                elif p["t"] == "status":
                    topic = "/" + self.config["customer"] + "/" + self.commonName + "/status"
                    payload = json.dumps({"v": p["v"], "timestamp": int(round(time.time() * 1000))})  # Nadaj timestamp
                    retval = self._mqttc.publish(topic, payload, qos=0)
                    if retval[0] == mqtt.MQTT_ERR_NO_CONN:
                        self.log.warn("No connection to mqtt server. PDU not sent. (%s)" % p)
                        self.failedPduCounter += 1
                        self.log.debug("Total PDU failed (%d)" % self.failedPduCounter)
                        self.log.debug("Adding to retransmit queue...")
                        self.retransmitQueue.put(p)
                        return
                        # PDU trzeba włożyć ponownie do modbusRS2mqttPublisher lub dedykowanej
                        # kolejki retransmisyjnej. Może mieć większy sens, bo pozwoli śledzić oddzielnie
                        # retransmitowane PDU. Zeby retransmisja dzialala, to ioxfeederd musi wysylac
                        # do influxdb dane opatrzone timestamp z mqtt. Inaczej wszystkie zretransmitowane
                        # PDU dostaną bieżący timestamp.
                        # AKTUALIZACJA: wszystkie moduły nadają timestamp. ioxfeederd wkłada do influxdb zgodnie
                        # z timestampem. Można wrzucać pakiety do kolejki retransmisyjnej. Kolejki zmienione
                        # na typy persistent. Wytrzymują przerwanie pracy applikacji i w razie czego również awarię
                        # zasilania.
                else:
                    self.log.warn("Unknown message from modbusRs received...")

        if not self.gpsReader2mqttPublisher.empty():
            #            self.log.debug("Processing data from gpsReader")
            p = self.gpsReader2mqttPublisher.get()
            #            self.log.debug(p)
            if p["t"] == "position":
                topic = "/" + self.config["customer"] + "/" + self.commonName + "/0/sensor/G.ps"
                payload = json.dumps({"v": p["v"], "timestamp": int(datetime.datetime.today().strftime('%s'))})  # Nadaj timestamp
                self._mqttc.publish(topic, payload)
            else:
                self.log.warn("Unknown message from gpsReader received...")

        if not self.tempReader2mqttPublisher.empty():
            p = self.tempReader2mqttPublisher.get()
            if p["t"] == "temperature":
                topic = "/" + self.config["customer"] + "/" + self.commonName + "/0/sensor/R.tmp"
                payload = json.dumps({"v": p["v"], "timestamp": int(datetime.datetime.today().strftime('%s'))})  # Nadaj timestamp
                self._mqttc.publish(topic, payload)
            elif p["t"] == "snmp":
                topic = "/" + self.config["customer"] + "/" + self.commonName + "/0/sensor/S.nmp"
                payload = json.dumps({"v": p["v"], "timestamp": int(datetime.datetime.today().strftime('%s'))})  # Nadaj timestamp
                self._mqttc.publish(topic, payload)
            else:
                self.log.warn("Unknown message from tempReader received...")

    def getCommonName(self):
        return configuration["hostname"]
