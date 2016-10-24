from Queue import Queue
import threading
import logging
import time
import paho.mqtt.client as mqtt
import socket
from config import configuration

class mqttSubscriber (threading.Thread):
    def __init__(self, threadID, name, config, mqttSubscriber2modbusRs, mqttSubscriber2updater):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.config = config
        self.log = logging.getLogger("mqttSubscriber")
        self._mqttc = mqtt.Client(client_id = configuration["hostname"] + "_sub")
        self._mqttc.max_inflight_messages_set(100)
        self._mqttc.on_message = self.mqtt_on_message
        self._mqttc.on_connect = self.mqtt_on_connect
        self._mqttc.on_publish = self.mqtt_on_publish
        self._mqttc.on_subscribe = self.mqtt_on_subscribe
        self._mqttc.on_log = self.mqtt_on_log
        self._mqttc.tls_set(self.config["caFile"],self.config["certFile"],self.config["keyFile"])
        self.mqttSubscriber2modbusRs = mqttSubscriber2modbusRs
        self.mqttSubscriber2updater = mqttSubscriber2updater
        self.tick = 0.01
        self.interval = 2
        self.exitFlag = False

    def mqtt_on_log(self, mqttc, userdata, level, buf):
        #self.log.debug("mqtt_on_log(): " + buf)
        pass

    def mqtt_on_connect(self, mqttc, obj, flags, rc):
        self.log.debug("on_connect() called")
        self._mqttc.subscribe("/"+self.config["customer"] + "/"+self.getCommonName()+"/set/+", 0)
        self._mqttc.subscribe("/"+self.config["customer"] + "/"+self.getCommonName()+"/get/+", 0)
        self._mqttc.subscribe("/"+self.config["customer"] + "/"+self.getCommonName()+"/update/+", 0)
        self.log.debug("/"+self.config["customer"] + "/"+self.getCommonName()+"/set/+")
        self.log.debug("/"+self.config["customer"] + "/"+self.getCommonName()+"/get/+")
        self.log.debug("/"+self.config["customer"] + "/"+self.getCommonName()+"/update/+")

    def mqtt_on_message(self,mosq, obj, msg):
        self.log.debug("on_message() called")
        global message
        self.log.debug(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))
        message = msg.payload
        command = msg.topic.split("/")[3]

        if command in ["get", "set"]:
            self.mqttSubscriber2modbusRs.put(msg)
        elif command in ["update"]:
            self.mqttSubscriber2updater.put(msg)
        else:
            self.log.warn("Unknown command (%s)" % (command))

        self.messageTime = int(time.time())

    def mqtt_on_publish(self,mosq, obj, mid):
        self.log.debug("mid: " + str(mid))

    def mqtt_on_subscribe(self,mosq, obj, mid, granted_qos):
        self.log.debug("on_subscribed() called")
        self.log.debug("Subscribed: " + str(mid) + " " + str(granted_qos))

    def run(self):
        self.log.debug("Starting " + self.name)
        while True:
            self.mqttConnect()
            while True:
                try:
                    r = self._mqttc.loop(timeout=1.0)
                    if r == mqtt.MQTT_ERR_CONN_LOST :
                        break
                except Exception as e:
                    self.log.debug(e)
                    self.log.debug("Connection broken")
                    break
                if self.exitFlag:
                    break
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
                time.sleep(5)
        self.log.debug("mqttConnect(): (Re)connected")

    def getCommonName(self):
        return configuration["hostname"]