# -*- coding: utf-8 -*-

from Queue import Queue
import threading
import time
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import requests
import json
import MySQLdb
import logging
import traceback
import sys

requests.packages.urllib3.disable_warnings()
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

class mqttInfluxdb(threading.Thread):
    def __init__(self, threadID, name, config):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.config = config
        self.log = logging.getLogger(name)
        self._mqttc = mqtt.Client(client_id="ioxfeederd", clean_session=False)
        self._mqttc.on_message = self.mqtt_on_message
        self._mqttc.on_connect = self.mqtt_on_connect
        self._mqttc.on_publish = self.mqtt_on_publish
        self._mqttc.on_subscribe = self.mqtt_on_subscribe
        self._mqttc.tls_set(self.config["mqtt"]["caFile"], self.config["mqtt"]["certFile"],
                            self.config["mqtt"]["keyFile"])
        self.mycursor = 0
        self.mysqldb = 0
        self.reconnect()
        self.tick = 0.01
        self.interval = 2
        self.exitFlag = False
        self.messageTime = int(time.time())
        self.influxdb = InfluxDBClient(host=self.config["influxdb"]["host"], port=self.config["influxdb"]["port"],
                                       username=self.config["influxdb"]["user"],
                                       password=self.config["influxdb"]["pass"],
                                       database=self.config["influxdb"]["db"], ssl=self.config["influxdb"]["ssl"])

    def reconnect(self):
        self.mysqldb = MySQLdb.connect(self.config["mysqldb"]["host"], self.config["mysqldb"]["user"],
                                       self.config["mysqldb"]["pass"], self.config["mysqldb"]["db"],
                                       connect_timeout=10)
        self.mycursor = self.mysqldb.cursor()

    def mqtt_on_connect(self, mqttc, obj, flags, rc):
        self.log.debug("on_connect() called")
        self._mqttc.subscribe("/" + self.config["customer"] + "/+/+/sensor/+", 0)
        self.log.debug("SUBSCRIBED TO: " + "/" + self.config["customer"] + "/+/+/sensor/+")
        self._mqttc.subscribe("/" + self.config["customer"] + "/+/status", 0)
        self.log.debug("SUBSCRIBED TO: " + "/" + self.config["customer"] + "/+/status")

    def mqtt_on_message(self, mosq, obj, msg):
        # self.log.debug("mqttInfluxdb:on_message() called")
        message = json.loads(str(msg.payload))
        # wszystko co nie jest int lub float nie raportuje
        siteId = msg.topic.split("/")[2]

        if msg.topic.split("/")[3] == "status":
            if "timestamp" in message:  ## od wersji 0.0.72 ioxclientd nadaje timestamp
                influxdbPdu = [{"measurement": "Status",
                                "tags": {"siteId": siteId},
                                "fields": {"value": int(message["v"])},
                                "time": message["timestamp"]
                                }]
            else:
                influxdbPdu = [{"measurement": "Status",
                                "tags": {"siteId": siteId},
                                "fields": {"value": int(message["v"])}
                                }]

            self.log.debug(influxdbPdu)
            sql = "UPDATE boilers SET status = %s WHERE router = %s"
            try:  # wysylamy status do influxdb
                self.influxdb.write_points(influxdbPdu, time_precision="s")
            except influxdb.exceptions.InfluxDBClientError as e:
                self.log.debug("mqtt_on_message(): influxdb server communication problems (%s)" % e)

            try:  # aktulizujemy status dla kotlowni
                self.mycursor.execute(sql, (int(message["v"]), siteId))
                self.mysqldb.commit()
            except MySQLdb.OperationalError as e:
                self.log.warn("mqtt_on_message() db error (%s)" % e)
                self.reconnect()
            except MySQLdb.Error as e:
                self.mysqldb.rollback()
                self.log.warn("mqtt_on_message() db error (%s)" % e)

        elif msg.topic.split("/")[5] == "G.ps":
            lat = message["v"]["lat"]
            lon = message["v"]["lon"]
            lat_dir = message["v"]["lat_dir"]
            lon_dir = message["v"]["lon_dir"]
            sql = "UPDATE boilers SET latitude = %s, latitudeDir = %s, longitude = %s, longitudeDir = %s WHERE router = %s"
            try:  # aktulizujemy pozycje dla kotlowni
                self.mycursor.execute(sql, (lat, lat_dir, lon, lon_dir, siteId))
                self.mysqldb.commit()
            except MySQLdb.OperationalError as e:
                self.log.warn("clearAlert() db error (%s)" % e)
                self.reconnect()
            except MySQLdb.Error as e:
                self.log.warn("mqtt_on_message Something went wrong %s" % e)
                self.mysqldb.rollback()

        elif msg.topic.split("/")[5] == "S.nmp":
            slave = msg.topic.split("/")[3]
            if "timestamp" in message:  ## od wersji 0.0.72 ioxclientd nadaje timestamp
                self.publish("Env.tmp", slave, siteId, int(message["v"]["env_temp"]), message["timestamp"])
                self.publish("R.ssi", slave, siteId, int(message["v"]["rssi"]), message["timestamp"])
            else:
                self.publish("Env.tmp", slave, siteId, int(message["v"]["env_temp"]))
                self.publish("R.ssi", slave, siteId, int(message["v"]["rssi"]))

        elif msg.topic.split("/")[5] in ["L.ck0", "L.ck1", "L.ck2", "L.ck3", "L.ck4", "L.ck5", "L.ck6", "L.ck7",
                                         "L.ck8"]:
            router = msg.topic.split("/")[2]
            date = '%d-%d-%d %d:%d:%d' % (
                2000 + message["v"][6], message["v"][7], message["v"][8], message["v"][9], message["v"][10],
                message["v"][11])
            bv = (
                router,
                message["v"][0],  # ErrorCode
                message["v"][1],  # DiagnosticCode
                message["v"][2],  # ErrorClass
                message["v"][3],  # ErrorPhase
                message["v"][4],  # Fuel
                message["v"][5],  # Output
                date,  # edate
                message["v"][12],  # StartupCounter
                message["v"][13]  # hoursTotal
            )
            sql = "CALL kotlownie.insert_lockdown(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            try:  # dodaj lockdown event
                self.mycursor.execute(sql, bv)
                self.mysqldb.commit()
            except MySQLdb.OperationalError as e:
                self.log.warn("clearAlert() db error (%s)" % e)
                self.reconnect()
            except MySQLdb.Error as e:
                self.log.warn("mqtt_on_message() Something went wrong (%s)" % e)
                self.mysqldb.rollback()

        elif type(message["v"]) is int or type(message["v"]) is float:
            register = msg.topic.split("/")[5]
            slave = msg.topic.split("/")[3]
            if "timestamp" in message:  ## od wersji 0.0.72 ioxclientd nadaje timestamp
                influxdbPdu = [{"measurement": register,
                                "tags": {"siteId": siteId, "slave": slave},
                                "fields": {"value": message["v"]},
                                "time": message["timestamp"]
                                }]
            else:
                influxdbPdu = [{"measurement": register,
                                "tags": {"siteId": siteId, "slave": slave},
                                "fields": {"value": message["v"]}
                                }]
            self.log.debug(influxdbPdu)
            try:
                self.influxdb.write_points(influxdbPdu, time_precision="s")
            except influxdb.exceptions.InfluxDBClientError as e:
                self.log.debug("mqtt_on_message(): influxdb server communication problems (%s)" % e)

        elif msg.topic.split("/")[5] == "ERR":
            pass

        else:
            self.log.warn("Unknown message received (%s)" % msg.topic.split("/")[5])

    def publish(self, register, slave, siteId, what, timestamp=0):
        if timestamp > 0:
            influxdbPdu = [{"measurement": register,
                            "tags": {"siteId": siteId, "slave": slave},
                            "fields": {"value": what},
                            "time": timestamp
                            }]
        else:
            influxdbPdu = [{"measurement": register,
                            "tags": {"siteId": siteId, "slave": slave},
                            "fields": {"value": what}
                            }]

        self.log.debug(influxdbPdu)
        try:
            self.influxdb.write_points(influxdbPdu, time_precision="s")
        except influxdb.exceptions.InfluxDBClientError as e:
            self.log.debug("mqtt_on_message(): influxdb server communication problems (%s)" % e)

    def mqtt_on_publish(self, mosq, obj, mid):
        self.log.debug("mid: " + str(mid))

    def mqtt_on_subscribe(self, mosq, obj, mid, granted_qos):
        self.log.debug("on_subscribed() called")
        self.log.debug("Subscribed: " + str(mid) + " " + str(granted_qos))

    def on_log(self, mosq, obj, level, string):
        self.log.debug(string)

    def run(self):
        self.log.info("Starting " + self.name)
        self._mqttc.connect(self.config["mqtt"]["host"], self.config["mqtt"]["port"],
                            self.config["mqtt"]["keepAlive"])
        self._mqttc.loop_start()
        while True:
            if self.exitFlag:
                break
            # self.mqttConnect()
            # while True:
            #     try:
            #         r = self._mqttc.loop(timeout=1.0)
            #         if r == mqtt.MQTT_ERR_CONN_LOST:
            #             self.log.warn("run() MQTT_ERR_CONN_LOST (%s)" % mqtt.error_string(r))
            #             break
            #         elif r != mqtt.MQTT_ERR_SUCCESS:
            #             self.log.warn("run() !MQTT_ERR_SUCCESS (%s)" % mqtt.error_string(r))
            #     except ValueError as e:
            #         self.log.debug("run(): (%s)" % e)
            #         for frame in traceback.extract_tb(sys.exc_info()[2]):
            #             fname, lineno, fn, text = frame
            #             self.log.debug("run() Error in %s on line %d" % (fname, lineno))
            #         break
            #     except Exception as e:
            #         self.log.debug("run(): MQTT server connection broken (%s)" % e.__class__)
            #         for frame in traceback.extract_tb(sys.exc_info()[2]):
            #             fname, lineno, fn, text = frame
            #             self.log.debug("Error in %s on line %d" % (fname, lineno))
            #         break
            #     if self.exitFlag:
            #         break
            #     time.sleep(self.tick)
            # if self.exitFlag:
            #     break
        self._mqttc.loop_stop()
        self.mycursor.close()
        self.mysqldb.close()
        self.log.info("Exiting " + self.name)

    def mqttConnect(self):
        self.log.debug("mqttConnect(): Trying to (re)connect")
        notConnected = True
        while notConnected:
            try:
                self._mqttc.connect(self.config["mqtt"]["host"], self.config["mqtt"]["port"],
                                    self.config["mqtt"]["keepAlive"])
                notConnected = False
                self.messageTime = int(time.time())
            except socket.error as e:
                self.log.debug("Socket Error: " + str(e))
                time.sleep(30)
            except Exception as e:
                self.log.debug("Something bad has happened. " + str(e))
                self.log.error(e, exc_info=True)
                time.sleep(30)
        self.log.debug("mqttConnect(): (Re)connected")

