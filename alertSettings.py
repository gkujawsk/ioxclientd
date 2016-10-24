from Queue import Queue
import threading
import time
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import requests
import json
import MySQLdb
requests.packages.urllib3.disable_warnings()

class mqttInfluxdb (threading.Thread):
    def __init__(self, threadID, name, config):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.config = config
        self._mqttc = mqtt.Client(None)
        self._mqttc.on_message = self.mqtt_on_message
        self._mqttc.on_connect = self.mqtt_on_connect
        self._mqttc.on_publish = self.mqtt_on_publish
        self._mqttc.on_subscribe = self.mqtt_on_subscribe
        #self._mqttc.on_log = self.mqtt_on_log
        self._mqttc.tls_set(self.config["mqtt"]["caFile"],self.config["mqtt"]["certFile"],self.config["mqtt"]["keyFile"])
        self.tick = 0.01
        self.interval = 2
        self.exitFlag = False
        self.messageTime = int(time.time())
        self.influxdb = InfluxDBClient(host=self.config["influxdb"]["host"], port=self.config["influxdb"]["port"],
                                       username=self.config["influxdb"]["user"], password=self.config["influxdb"]["pass"],
                                       database=self.config["influxdb"]["db"], ssl=self.config["influxdb"]["ssl"])
        self.mysqldb = MySQLdb.connect(self.config["mysqldb"]["host"], self.config["mysqldb"]["user"],
                                       self.config["mysqldb"]["pass"], self.config["mysqldb"]["db"],
                                       connect_timeout=10)
        self.mycursor = self.mysqldb.cursor()

    def mqtt_on_log(self, mqttc, userdata, level, buf):
        #print("mqttInfluxdb:mqtt_on_log(): " + buf)
        pass

    def mqtt_on_connect(self, mqttc, obj, flags, rc):
        print("on_connect() called")
        self._mqttc.subscribe("/" + self.config["customer"] + "/+/+/sensor/+", 0)
        print("SUBSCRIBED TO: " + "/" + self.config["customer"] + "/+/+/sensor/+")
        self._mqttc.subscribe("/" + self.config["customer"] + "/+/status", 0)
        print("SUBSCRIBED TO: " + "/" + self.config["customer"] + "/+/status")
        print("rc: " + str(rc))

    def mqtt_on_message(self,mosq, obj, msg):
        #print("mqttInfluxdb:on_message() called")
        message = json.loads(str(msg.payload))
        # wszystko co nie jest int lub float nie raportuje
        siteId = msg.topic.split("/")[2]
#        slave = msg.topic.split("/")[3]
        if msg.topic.split("/")[3] == "status":
            influxdbPdu = [{"measurement" : "Status",
                            "tags":{"siteId":siteId},
                            "fields":{"value":int(message["v"])}
                           }]
            print(influxdbPdu)
            sql = "UPDATE boilers SET status = %s WHERE router = %s"
            try: # wysylamy status do influxdb
                self.influxdb.write_points(influxdbPdu)
            except influxdb.exceptions.InfluxDBClientError as e:
                print("mqttInfluxdb:mqtt_on_message(): influxdb server communication problems")

            try: # aktulizujemy status dla kotlowni
               self.mycursor.execute(sql, (int(message["v"]), siteId))
               self.mysqldb.commit()
            except:
               self.mysqldb.rollback()
	       

        elif msg.topic.split("/")[5] == "G.ps":
	    lat = message["v"]["lat"]
	    lon = message["v"]["lon"]
	    lat_dir = message["v"]["lat_dir"]
	    lon_dir = message["v"]["lon_dir"]
            sql = "UPDATE boilers SET latitude = %s, latitudeDir = %s, longitude = %s, longitudeDir = %s WHERE router = %s"
            try: # aktulizujemy pozycje dla kotlowni
            	self.mycursor.execute(sql, (lat , lat_dir, lon, lon_dir , siteId))
            	self.mysqldb.commit()
            except MySQLdb.Error as e:
	    	print("\t\t\t\tSomething went wrong %s" % (err))
                self.mysqldb.rollback()

	elif msg.topic.split("/")[5] == "S.nmp":
            register = msg.topic.split("/")[5]
            slave = msg.topic.split("/")[3]
	    self.publish("Env.tmp", slave, siteId, int(message["v"]["env_temp"]))
	    self.publish("R.ssi", slave, siteId, int(message["v"]["rssi"]))
	    pass

	elif msg.topic.split("/")[5] in ["L.ck0","L.ck1","L.ck2","L.ck3","L.ck4","L.ck5","L.ck6","L.ck7","L.ck8"]:
	    router = msg.topic.split("/")[2]
	    date =  '%d-%d-%d %d:%d:%d' % (2000+message["v"][6], message["v"][7], message["v"][8], message["v"][9], message["v"][10], message["v"][11])
            bv = (
		router,
		message["v"][0],  # ErrorCode
		message["v"][1],  # DiagnosticCode
		message["v"][2],  # ErrorClass
		message["v"][3],  # ErrorPhase
		message["v"][4],  # Fuel
		message["v"][5], # Output 
		date, # edate
		message["v"][12], #StartupCounter
		message["v"][13] #hoursTotal
            )
	    sql = "CALL kotlownie.insert_lockdown(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
	    print(bv)
            try: # dodaj lockdown event
            	self.mycursor.execute(sql, bv)
            	self.mysqldb.commit()
            except Exception as e:
	    	print("\t\t\t\tSomething went wrong (%s)" % (e))
                self.mysqldb.rollback()
	    else:
                print "Wszystko poszlo dobrze"

        elif (type(message["v"]) is int or type(message["v"]) is float):
            register = msg.topic.split("/")[5]
            slave = msg.topic.split("/")[3]
            influxdbPdu = [{"measurement" : register,
                            "tags":{"siteId":siteId,"slave":slave},
                            "fields":{"value":message["v"]}
                           }]
            print(influxdbPdu)
            try:
                self.influxdb.write_points(influxdbPdu)
            except influxdb.exceptions.InfluxDBClientError as e:
                print("mqttInfluxdb:mqtt_on_message(): influxdb server communication problems")
        else:
            pass
    
    def publish(self,register, slave, siteId, what):
   	influxdbPdu = [{"measurement" : register,
		    "tags":{"siteId":siteId,"slave":slave},
		    "fields":{"value":what}
		   }]
    	print(influxdbPdu)
    	try:
	    self.influxdb.write_points(influxdbPdu)
    	except influxdb.exceptions.InfluxDBClientError as e:
	    print("mqttInfluxdb:mqtt_on_message(): influxdb server communication problems")

    def mqtt_on_publish(self,mosq, obj, mid):
        print("mid: " + str(mid))

    def mqtt_on_subscribe(self,mosq, obj, mid, granted_qos):
        print("on_subscribed() called")
        print("Subscribed: " + str(mid) + " " + str(granted_qos))

    def on_log(self, mosq, obj, level, string):
        print(string)

    def run(self):
        print("Starting " + self.name)
        while True:
            self.mqttConnect()
            while True:
                try:
                    r = self._mqttc.loop(timeout=1.0)
                    if(r == mqtt.MQTT_ERR_CONN_LOST):
                        break
                except Exception as e:
                    print("mqttInfluxdb:run(): MQTT server connection broken")
                    break
                if self.exitFlag:
                    break
                time.sleep(self.tick)
            if self.exitFlag:
                break
        print("Exiting " + self.name)

    def mqttConnect(self):
        print("mqttInfluxdb:mqttConnect(): Trying to (re)connect")
        notConnected = True
        while notConnected:
            try:
                self._mqttc.connect(self.config["mqtt"]["host"], self.config["mqtt"]["port"], self.config["mqtt"]["keepAlive"])
                notConnected = False
                self.messageTime = int(time.time())
            except Exception as e:
                print("mqttInfluxdb:mqttConnect(): exception cought " + str(e))
                time.sleep(5)
        print("mqttInfluxdb:mqttConnect(): (Re)connected")

    def getCommonName(self):
        cn = None
        with open(self.config["mqtt"]["certFile"],"rb") as f:
            der = f.read()
        import OpenSSL.crypto
        x509 = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_PEM, der)
        subject = x509.get_subject().get_components()
        for i in subject:
            if i[0] == "CN":
                cn = i[1]
        return cn
