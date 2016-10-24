# -*- coding: utf-8 -*-

from Queue import Queue
import threading
import logging
import time
import paho.mqtt.client as mqtt
from config import configuration
import json
import smtplib
import MySQLdb
import MySQLdb.cursors
import requests
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
requests.packages.urllib3.disable_warnings()




# Tutaj w config bezpośrednio odnośnik do configuration a nie elementu.
class alertSubscriber(threading.Thread):
    def __init__(self, threadID, name, config):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.config = config[name]
        self.log = logging.getLogger(name)
        self._mqttc = mqtt.Client(client_id=config["hostname"])
        self._mqttc.on_message = self.mqtt_on_message
        self._mqttc.on_connect = self.mqtt_on_connect
        self._mqttc.on_publish = self.mqtt_on_publish
        self._mqttc.on_subscribe = self.mqtt_on_subscribe
        self._mqttc.on_log = self.mqtt_on_log
        self._mqttc.tls_set(config["ssl"]["caFile"], config["ssl"]["certFile"], config["ssl"]["keyFile"])
        self.reconnect()
        self.tick = 0.01
        self.interval = 2
        self.exitFlag = False
        self.raised = {}
        self.mails = {}
        self.phones = {}
        self.slacks = {}

    def reconnect(self):
        self.mysqldb = MySQLdb.connect(self.config["mysqldb"]["host"], self.config["mysqldb"]["user"],
                                       self.config["mysqldb"]["pass"], self.config["mysqldb"]["db"],
                                       cursorclass=MySQLdb.cursors.DictCursor,
                                       connect_timeout=10)
        self.mycursor = self.mysqldb.cursor()

    def mqtt_on_log(self, mqttc, userdata, level, buf):
        self.log.debug("mqtt_on_log(): " + buf)

    def mqtt_on_connect(self, mqttc, obj, flags, rc):
        self.log.debug("on_connect() called with code %d" %(rc))
        if rc == 0:
            self._mqttc.subscribe("/" + self.config["customer"] + "/+/+/sensor/+", 0)
            self.log.info("SUBSCRIBED TO: " + "/" + self.config["customer"] + "/+/+/sensor/+")
            self._mqttc.subscribe("/" + self.config["customer"] + "/+/status", 0)
            self.log.info("SUBSCRIBED TO: " + "/" + self.config["customer"] + "/+/status")
        else:
            self.log.info("Cannot connect to mqtt server.")

    def mqtt_on_message(self, mosq, obj, msg):
        #        self.log.debug("on_message() called")
        global message
        #        self.log.debug(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))
        try:
            message = json.loads(msg.payload)
        except ValueError:
            self.log.warn("mqtt_on_message(): Message malformed. Skipping.")
            return

        router = msg.topic.split("/")[2]
        command = msg.topic.split("/")[3]
        try:
            sensor = msg.topic.split("/")[4]
            _register = msg.topic.split("/")[5]
        except IndexError:
            sensor = False

        registers = {}

        if (command == "status"):  # gdy odebrano informacje o statusie
            #            self.log.debug("GOT STATUS")
            pass
        elif (sensor == "sensor"):  # gdy mamy dane o stanie sensora
            #            self.log.debug("GOT SENSOR")
            if _register == "S.nmp":  # Trzeba rozkodować zawartość
                if "env_temp" in message["v"]:
                    registers["Env.tmp"] = int(message["v"]["env_temp"])
                if "rssi" in message["v"]:
                    registers["R.ssi"] = int(message["v"]["rssi"])
            else:  # Nie trzeba rozkodowywać zawartości, wartość bezpośrednio pod message["v"]
                registers[_register] = message["v"]
                #currentValue = message["v"]

            for register in registers:
                currentValue = registers[register]
                if router in self.config["thresholds"]:  # dla router ustawiony jest próg
                    if register in self.config["thresholds"][router]:
                        for i, t in enumerate(self.config["thresholds"][router][register]):
                            self.log.debug("Checking (%s, %s, %s)" % (router, register, t["op"]))
                            if t["op"] == ">":  # czy wartość wieksza od zadanej?
                                if currentValue > t["threshold"]:  # wartość większa od zadanej
                                    self.trigger(router, register, i, sensor, currentValue, t)  # obsługa progu, trigger
                                else:  # wartosc ponizej progu
                                    self.clear(router, register, i, sensor, currentValue, t)  # obsługa progu, clear
                            elif t["op"] == "<":  # czy wartość mniejsza od zadanej?
                                if currentValue < t["threshold"]:  # wartość mniejsza od zadanej
                                    self.trigger(router, register, i, sensor, currentValue, t)  # obsługa progu, trigger
                                else:  # wartość poniżej progu
                                    self.clear(router, register, i, sensor, currentValue, t)  # obsługa progu, clear
                            elif t["op"] == "==":  # czy wartość mniejsza od zadanej?
                                if currentValue == t["threshold"]:  # wartość mniejsza od zadanej
                                    self.trigger(router, register, i, sensor, currentValue, t)  # obsługa progu, trigger
                                else:  # wartość poniżej progu
                                    self.clear(router, register, i, sensor, currentValue, t)  # obsługa progu, clear
                            elif t["op"] == ">=":  # czy wartość mniejsza od zadanej?
                                if currentValue >= t["threshold"]:  # wartość mniejsza od zadanej
                                    self.trigger(router, register, i, sensor, currentValue, t)  # obsługa progu, trigger
                                else:  # wartość poniżej progu
                                    self.clear(router, register, i, sensor, currentValue, t)  # obsługa progu, clear
                            elif t["op"] == "<=":  # czy wartość mniejsza od zadanej?
                                if currentValue <= t["threshold"]:  # wartość mniejsza od zadanej
                                    self.trigger(router, register, i, sensor, currentValue, t)  # obsługa progu, trigger
                                else:  # wartość poniżej progu
                                    self.clear(router, register, i, sensor, currentValue, t)  # obsługa progu, clear
                            else:
                                self.log.debug("Unknown op (%s)" % (t["op"]))
        else:
            self.log.debug("UNKNOWN message")

    def clear(self, router, register, i, sensor, message, t):
        self.log.debug("Value below the threshold.")
        try:
            lastId = self.raised[router][register][i]["id"]
        except KeyError as e:
            self.log.debug("Alarm not raised. Doing nothing.")
        else:
            self.log.debug("Alarm already raised. Going to clear the status for id %s" % lastId)
            self.notify(router, sensor, register, message, i, "OK", False)
            self.clearAlert(lastId)
            del self.raised[router][register][i]  # alarm wyczyszczony

    def trigger(self, router, register, i, sensor, message,t):
        try:
            if self.raised[router][register][i]:
                self.log.debug("Checking notification. Already raised (%s, %s, %s, %d, %s)" %
                               (router, register, t["op"], t["threshold"], message))
                self.log.debug(
                    "Raised %d seconds ago" % (
                        time.time() - self.raised[router][register][i]["time"]))
                if time.time() - self.raised[router][register][i]["time"] > t["suppress"]:

                    #  self.notify(router, sensor, register, message, i)
                    lastId = self.raised[router][register][i]["id"]
                    count = self.raised[router][register][i]["count"]
                    self.log.debug("Going to update the repeat couter by %s for %s",
                                   count, lastId)
                    self.updateAlertCounter(lastId, count)
                    self.raised[router][register][i] = {"time": time.time(), "count": 0, "id": lastId}
                else:
                    self.raised[router][register][i]["count"] += 1
                    self.log.debug("Suppressing couter update. Current count: %d" %
                                   self.raised[router][register][i]["count"])
        except KeyError as e:  # Klucz nie istniał, więc alarm nie włączony (albo brak router lub register lub i.
            self.log.debug("Raising notification (%s, %s, %s)" % (router, register, t["op"]))
            try:
                self.raised[router]  # To znaczy, że ani razu nie badano jeszcze routera
            except KeyError:
                self.raised[router] = {}  # Utwórz słownik dla router

            try:
                self.raised[router][register]  # To znaczy, że tego rejestru jeszcze nie śledzimy
            except KeyError:
                self.raised[router][register] = {} # Utwórz słownik dla tego rejestru


            self.raised[router][register][i] = {"time": time.time(), "count": 0}
            self.notify(router, sensor, register, message, i, "PROBLEM")

    def mqtt_on_publish(self, mosq, obj, mid):
        self.log.debug("mid: " + str(mid))

    def mqtt_on_subscribe(self, mosq, obj, mid, granted_qos):
        self.log.debug("on_subscribed() called")
        self.log.debug("Subscribed: " + str(mid) + " " + str(granted_qos))

    def mqtt_on_log(self, mosq, obj, level, string):
        #        self.log.debug(string)
        pass

    def run(self):
        self.log.info("Starting " + self.name)
        self._mqttc.connect(self.config["host"], self.config["port"], self.config["keepAlive"])
        self._mqttc.loop_start()
        while True:
            if self.exitFlag:
                break
        self._mqttc.loop_stop()
        self.mycursor.close()
        self.mysqldb.close()
        self.log.info("Exiting " + self.name)

    def mqttConnect(self):
        self.log.debug("mqttConnect(): Trying to (re)connect")
        notConnected = True
        while notConnected:
            try:
                notConnected = self._mqttc.connect(self.config["host"],
                                                   self.config["port"],
                                                   self.config["keepAlive"])
            except Exception as e:
                self.log.debug("mqttConnect(): exception cought " + str(e))
                time.sleep(5)
        self.log.debug("mqttConnect(): (Re)connected")

    def clearAlert(self, lastId):
        self.log.debug("clearAlert() called")
        try:
            self.mycursor.execute("CALL clear_alert(%s)", (lastId,))
            self.mycursor.nextset()
            self.mysqldb.commit()
        except MySQLdb.Error as e:
            self.log.warn("clearAlert() error (%s)" % e)

    def updateAlertCounter(self, lastId,count):
        self.log.debug("clearAlert() called")
        try:
            self.mycursor.execute("CALL update_alert_count(%s, %s)", (lastId,count))
            self.mycursor.nextset()
            self.mysqldb.commit()
        except MySQLdb.OperationalError as e:
            self.log.warn("clearAlert() db error (%s)" % e)
            self.reconnect()
        except MySQLdb.Error as e:
            self.log.warn("clearAlert() error (%s)" % e)

    def notify(self, router, sensor, register, value, i,prefix,notifyDb=True):
        self.log.debug("notify() called")
        self.mails = {"group":[],"customer":[],"service":[]}
        self.phones = {"group":[],"customer":[],"service":[]}
        self.slacks = {"group":[],"customer":[],"service":[]}
        count = self.raised[router][register][i]["count"]
        alertMsg = {"sensor": sensor, "register": register, "value": value,
                    "count": count, "i": i, "router": router, "prefix": prefix}
        if notifyDb:
            self.notifyDb(alertMsg)

        alertMsg["id"] = self.raised[router][register][i]["id"]

        if "notify" in self.config["thresholds"][router][register][i]:
            notifies = self.config["thresholds"][router][register][i]["notify"]
            for i in notifies:
                if i == "customer" or i == "service":
                    if "sms" in notifies[i]:
                        self.collectPhones(i, router, alertMsg)
                    if "email" in notifies[i]:
                        self.collectEmails(i, router, alertMsg)
                    if "slack" in notifies[i]:
                        self.collectSlackRooms(i, router, alertMsg)
                elif i == "groups":
                     groups = {"sms": [], "email": [], "slack": []}
                     for g in notifies[i]:
                         if "sms" in notifies[i][g]:
                             groups["sms"].append(g)
                         if "email" in notifies[i][g]:
                             groups["email"].append(g)
                         if "slack" in notifies[i][g]:
                             groups["slack"].append(g)
                     self.collectPhones(groups["sms"], router, alertMsg)
                     self.collectEmails(groups["email"], router, alertMsg)
                     self.collectSlackRooms(groups["slack"], router, alertMsg)
                else:
                     self.log.warn("Unknown notification key for %s %s %s" % (router, register, i))

            dedupMails = sorted(list(set(self.mails["group"] + self.mails["customer"] + self.mails["service"])))
            self.notifyEmail(dedupMails, alertMsg)
            dedupPhones = sorted(list(set(self.phones["group"] + self.phones["customer"] + self.phones["service"])))
            self.notifySms(dedupPhones, alertMsg)
        else:
            self.log.warn("No notification method set for %s:%s" %(router, register))

    def notifyDb(self, alertMsg):
        self.log.debug("notifyDb() called")
        sql = "CALL insert_alert(%s , %s, %s, %s, %s, %s)"
        router = alertMsg["router"]
        register = alertMsg["register"]
        i = alertMsg["i"]
        count = alertMsg["count"]
        sensor = alertMsg["sensor"]
        value = alertMsg["value"]
        text = "Register: %s\nAlarm type: %s\nThreshold: %d\nValue: %s\nAddres: %s\n" \
               %(register,
                 self.config["thresholds"][router][register][i]["op"],
                 self.config["thresholds"][router][register][i]["threshold"],
                 value, sensor)
        if "severity" in self.config["thresholds"][router][register][i]:
            severity = self.config["thresholds"][router][register][i]["severity"]
        else:
            severity = "info"

        bv = (router, register, severity, text, "active", count)
        try:
            self.mycursor.execute(sql, bv)
            result = self.mycursor.fetchone()
            self.raised[router][register][i]["id"] = result["lastId"]  # id w tabeli alerts, mozna zmienic status alarmu
            self.mycursor.nextset()
            self.mysqldb.commit()
        except MySQLdb.OperationalError as e:
            self.log.warn("notifyDb() db error (%s)" % e)
            self.reconnect()
        except MySQLdb.Error as e:
            self.log.warn("notifyDb() error (%s) " % e)
        else:
            self.log.debug("notifyDb() alert inserted properly on id (%s)" % result["lastId"])
        pass

    # kind: [customer,service,username]
    def collectEmails(self, kind, router, alertMsg):
         #self.log.debug("collectEmails() called for kind (%s) and router (%s)" % (kind, router))
         if type(kind).__name__ in ('list'):
             self.mails["group"] = self.getGroupEmails(kind)
             #self.log.debug("\t\t\tGROUP EMAIL: %s" % self.mails["group"])
         elif kind == "customer":
             self.mails["customer"] = self.getCustomerEmails(router)
             #self.log.debug("\t\t\tCUSTOMER EMAILS: %s" % self.mails["customer"])
         elif kind == "service":
             self.mails["service"] = self.getServiceEmails(router)
             #self.log.debug("\t\t\tSERVICE EMAILS: %s" % self.mails["service"])
         else:
             self.log.debug("collectEmails() unknown kind (%s)" % kind)

    def notifyEmail(self, recepients, alertMsg):
        self.log.debug("notifyEmail() called with recepient list (%s)" % recepients)
        lockdowns = ""
        try:
            self.mycursor.execute("CALL getLockdowns(%s,%s)", (alertMsg["router"], 9))
        except MySQLdb.OperationalError as e:
            self.log.warn("notifyEmail() db error (%s)" % e)
            self.reconnect()
        except MySQLdb.Error as e:
            self.log.warn("notifyEmail() error (%s)" % e)
            lockdowns = "Błąd połączenia z bazą danych."
        else:
            results = self.mycursor.fetchall()
            for r in results:
                lockdowns += "Kod błędu: %s\tKod diagnostyczny: %s\tData: %s\n" % (r["errorCode"], r["diagnosticCode"],r["eDate"])
            self.mycursor.nextset()

        msg = MIMEMultipart('alternative')
        subject = "[CALDERA ALERT] #%s %s @ %s %s" % (alertMsg["id"], alertMsg["prefix"], alertMsg["router"], alertMsg["register"])
        msg["Subject"] = Header(subject,'utf-8')
        msg["From"] = "CALDERA <%s>" % self.config["smtp"]["from"]
        msg["To"] = ", ".join(recepients)

        body = self.getPlainBody(alertMsg, lockdowns)
        html = self.getHtmlBody(alertMsg, lockdowns)

        part1 = MIMEText(body, 'plain', "utf-8")
        part2 = MIMEText(html, 'html', "utf-8")

        msg.attach(part1)
        msg.attach(part2)


        try:
            server = smtplib.SMTP(host=self.config["smtp"]["host"], port=self.config["smtp"]["port"],timeout=10)
            server.ehlo()
            server.starttls()
            server.login(self.config["smtp"]["username"], self.config["smtp"]["password"])
        except smtplib.SMTPException as e:
            self.log.warn("collectEmails() error connecting to (%s:%s) (%s)" % (
            self.config["smtp"]["host"], self.config["smtp"]["port"], e))
        else:
            emailText = """\
From: CALDERA <%s>
To: %s
Subject: %s

%s
""" % (self.config["smtp"]["from"], ", ".join(recepients), subject, body)
            # server.sendmail(self.config["smtp"]["from"], recepients, emailText)
            server.sendmail(self.config["smtp"]["from"], recepients, msg.as_string())
            server.quit()

    def getPlainBody(self,alertMsg, lockdowns):
        body = """\
        ID alarmu: #%s
        Poziom alarmu: %s
        Rejestr: %s
        Router: %s
        Bieżący odczyt %s
        Zadany próg: %s
        Warunek progu: %s

        Historia ostatnich 9 lockdownów:
        %s

        https://mqtt.iox.com.pl/login

        """ % (alertMsg["id"],
               self.config["thresholds"][alertMsg["router"]][alertMsg["register"]][alertMsg["i"]]["severity"],
               alertMsg["register"], alertMsg["router"], alertMsg["value"],
               self.config["thresholds"][alertMsg["router"]][alertMsg["register"]][alertMsg["i"]]["threshold"],
               self.config["thresholds"][alertMsg["router"]][alertMsg["register"]][alertMsg["i"]]["op"],
               lockdowns
               )
        return body


    def getHtmlBody(self,alertMsg, lockdowns):
        lockdowns = "<br />".join(lockdowns.split("\n"))
        body = """\
<html>
<head></head>
<style>
table {
    border-collapse: collapse;
}
table, th, td {
   border: 1px solid black;
}
</style>
<body>
<H3> CALDERA ALARM </h3>
<table style="width:250px;">
<tr style="width:100px;">
<td><b>ID alarmu</b></td><td>#%s </td>
</tr>
<tr>
<td><b>Poziom alarmu</b></td><td> %s </td>
</tr>
<tr>
<td><b>Rejestr</b></td><td> %s</td>
</tr>
<tr>
<td><b>Router</b></td><td> %s</td>
</tr>
<tr>
<td><b>Bieżący odczyt</b></td><td> %s</td>
</tr>
<tr>
<td><b>Zadany próg</b></td><td>%s</td>
</tr>
<tr>
<td><b>Warunek progu</b></td><td> %s</td>
</tr>
</table><br ?>

<b>Historia ostatnich 9 lockdownów:</b><br />
%s
<br/>
Szczegóły <a href="https://mqtt.iox.com.pl/login">https://mqtt.iox.com.pl/login</a>
<br/>
<a href="http://www.wynajemkotlowni.pl/">
<img src="http://dev.graff.pl/customers/gpage/wynajemkotlowni/data/img/pl_PL/logo.png" style="width:200px;border:0px;"/>
</a>
</body>
</html>
        """ % (alertMsg["id"],
               self.config["thresholds"][alertMsg["router"]][alertMsg["register"]][alertMsg["i"]]["severity"],
               alertMsg["register"], alertMsg["router"], alertMsg["value"],
               self.config["thresholds"][alertMsg["router"]][alertMsg["register"]][alertMsg["i"]]["threshold"],
               self.config["thresholds"][alertMsg["router"]][alertMsg["register"]][alertMsg["i"]]["op"],
               lockdowns
               )
        return body

    def collectPhones(self, kind, router, alertMsg):
        #self.log.debug("collectPhones() called for kind (%s) and router (%s)" % (kind, router))
        if type(kind).__name__ in ('list'):
            self.phones["group"] = self.getGroupPhones(kind)
            #self.log.debug("\t\t\tGROUP PHONES: %s" % self.phones["group"])
        elif kind == "customer":
            self.phones["customer"] = self.getCustomerPhones(router)
            #self.log.debug("\t\t\tCUSTOMER PHONES: %s" % self.phones["customer"])
        elif kind == "service":
            self.phones["service"] = self.getServicePhones(router)
            #self.log.debug("\t\t\tSERVICE PHONES: %s" % self.phones["service"])
        else:
            self.log.warn("collectPhones() unknown kind (%s)" % kind)

    def notifySms(self, recepients, alertMsg):
        return True
        self.log.debug("notifySms() called with recepient list (%s)" % recepients)
        payload = {'username': self.config["smsapi"]["username"],
                   'password': self.config["smsapi"]["password"],
                   'to': "48608652741",
                   'message': "CALDERA ALERT %s %s:%s" % (alertMsg["router"], alertMsg["register"],alertMsg["value"])}
        try:
            requests.get(self.config["smsapi"]["url"], params=payload, timeout=0.5)
            pass
        except requests.ConnectionError as e:
            log.info("ioxclient.py:sms_alert(): Unable to send SMS alert %s" % (e.strerror))
        except requests.Timeout as e:
            log.info("ioxclient.py:sms_alert(): Connection to smsapi.pl timeout %s" % (e.strerror))

    def collectSlackRooms(self, kind, router, alertMsg):
        #self.log.debug("collectSlackRooms() called for kind %s" % (kind))
        pass

    # zwraca listę adresów email uzytkowników danego klienta przypisanego do danej kotłowni
    def getCustomerEmails(self, router):
        retval = []
        try:
            sql = "CALL getCustomerEmails(%s)"
            self.mycursor.execute(sql, (router, ))
            result = self.mycursor.fetchall()
            self.mycursor.nextset()
        except MySQLdb.OperationalError as e:
            self.log.warn("getCustomerEmail() db error (%s)" % e)
            self.reconnect()
        except MySQLdb.Error as e:
            self.log.warn("getCustomerEmail() error (%s) " %(e))
            retval = False
        else:
            for r in result:
                retval.append(r["email"])
        return retval

    # zwraca listę adresów email serwisantów przypisanych do danej kotłowni
    def getServiceEmails(self, router):
        retval = []
        try:
            sql = "CALL getServiceEmails(%s)"
            self.mycursor.execute(sql, (router, ))
            result = self.mycursor.fetchall()
            self.mycursor.nextset()
        except MySQLdb.OperationalError as e:
            self.log.warn("getServiceEmail() db error (%s)" % e)
            self.reconnect()
        except MySQLdb.Error as e:
            self.log.warn("getServiceEmail() error (%s) " %(e))
            retval = False
        else:
            for r in result:
                retval.append(r["email"])
        return retval

    # zwraca listę adresów email w grupach
    def getGroupEmails(self, groups):
        retval = []
        for g in groups:
            try:
                sql = "CALL getGroupEmails(%s)"
                self.mycursor.execute(sql, (g, ))
                result = self.mycursor.fetchall()
                self.mycursor.nextset()
            except MySQLdb.OperationalError as e:
                self.log.warn("getGroupEmails() db error (%s)" % e)
                self.reconnect()
            except MySQLdb.Error as e:
                self.log.warn("getGroupEmail() error (%s) " %(e))
                retval = False
            else:
                for r in result:
                    retval.append(r["email"])
        return retval

    # zwraca listę nr telefonów użytkowników danego klienta przypisanego do danej kotłowni
    def getCustomerPhones(self, router):
        retval = []
        try:
            sql = "CALL getCustomerPhones(%s)"
            self.mycursor.execute(sql, (router, ))
            result = self.mycursor.fetchall()
            self.mycursor.nextset()
        except MySQLdb.OperationalError as e:
            self.log.warn("getCustomerPhones() db error (%s)" % e)
            self.reconnect()
        except MySQLdb.Error as e:
            self.log.warn("getCustomerPhones() error (%s) " % (e))
            retval = False
        else:
            for r in result:
                self.log.debug(r)
                retval.append(r["mobile"])
        return retval

    # zwraca listę nr telefonów serwisantów przypisanych do danej kotłowni
    def getServicePhones(self, router):
        retval = []
        try:
            sql = "CALL getServicePhones(%s)"
            self.mycursor.execute(sql, (router, ))
            result = self.mycursor.fetchall()
            self.mycursor.nextset()
        except MySQLdb.OperationalError as e:
            self.log.warn("getServicePhones() db error (%s)" % e)
            self.reconnect()
        except MySQLdb.Error as e:
            self.log.warn("getServicePhones() error (%s) " %(e))
            retval = False
        else:
            for r in result:
                retval.append(r["mobile"])
        return retval

    # zwraca listę telefonów w grupach
    def getGroupPhones(self, groups):
        retval = []
        for g in groups:
            try:
                sql = "CALL getGroupPhones(%s)"
                self.mycursor.execute(sql, (g, ))
                result = self.mycursor.fetchall()
                self.mycursor.nextset()
            except MySQLdb.OperationalError as e:
                self.log.warn("getGroupPhones() db error (%s)" % e)
                self.reconnect()
            except MySQLdb.Error as e:
                self.log.warn("getGroupPhones() error (%s) " %(e))
                retval = False
            else:
                for r in result:
                    retval.append(r["mobile"])
        return retval

    # zwraca id kanału slack przypisanego do danego klienta
    def getCustomerSlack(self, router):
        pass

    # zwraca id kanału slack przypisanego do serwisantów
    def getServiceSlack(self, router):
        pass

    def getCommonName(self):
        return configuration["hostname"]
