from Queue import Queue
import threading
import time
import paho.mqtt.client as mqtt

class mqttSubscriber (threading.Thread):
    def __init__(self, threadID, name, config, mqttSubscriber2modbusRs):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.config = config
        self._mqttc = mqtt.Client(None)
        self._mqttc.on_message = self.mqtt_on_message
        self._mqttc.on_connect = self.mqtt_on_connect
        self._mqttc.on_publish = self.mqtt_on_publish
        self._mqttc.on_subscribe = self.mqtt_on_subscribe
        self._mqttc.tls_set(self.config["caFile"], self.config["certFile"], self.config["keyFile"])
        self.mqttSubscriber2modbusRs = mqttSubscriber2modbusRs
        self.tick = 0.01
        self.interval = 2
        self.exitFlag = False
        self.messageTime = int(time.time())

    def mqtt_on_connect(self, mqttc, obj, flags, rc):
        print("on_connect() called")
        self._mqttc.subscribe("/sensor/"+self.getCommonName()+"/temperatura", 0)
        print("rc: " + str(rc))

    def mqtt_on_message(self,mosq, obj, msg):
        print("on_message() called")
        global message
        print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))
        message = msg.payload
        self.mqttSubscriber2modbusRs.put(msg)
        self.messageTime = int(time.time())

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
                    self._mqttc.loop(timeout=1.0)
                except Exception as e:
                    print(e)
                    print("Connection broken")
                    break
                if(int(time.time()) - self.messageTime > self.config["keepAlive"] + 5):
                    print("30 seconds without any message")
                    break
                if self.exitFlag:
                    break
                time.sleep(self.tick)
            if self.exitFlag:
                break
        print("Exiting " + self.name)

    def mqttConnect(self):
        print("Trying to connect")
        notConnected = True
        while notConnected:
            try:
                self._mqttc.connect(self.config["host"], self.config["port"], self.config["keepAlive"])
                notConnected = False
                self.messageTime = int(time.time())
            except Exception as e:
                print(e)
                print("BRAK LACZNOSCI")
                time.sleep(5)
        print("Connected")

    def getCommonName(self):
        cn = None
        with open(self.config["certFile"],"rb") as f:
            der = f.read()
        import OpenSSL.crypto
        x509 = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_PEM, der)
        subject = x509.get_subject().get_components()
        for i in subject:
            if i[0] == "CN":
                cn = i[1]
        return cn