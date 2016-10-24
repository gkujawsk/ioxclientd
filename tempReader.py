# -*- coding: utf-8 -*-
import json,logging,time,sys
import threading
import constants
import config
import socket
from Queue import Queue
from pysnmp.entity.rfc3413.oneliner import cmdgen


class tempReader (threading.Thread):
    def __init__(self, threadID, name, config, tempReader2mqttPublisher):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.config = config
        self.log = logging.getLogger("tempReader")
        self.temp = True
        self.temp_sock = None
        self.temp_current = None
        self.tempReader2mqttPublisher = tempReader2mqttPublisher
        self.tick = 0.01
        self.interval = 0.5
        self.exitFlag = False
        if self.temp:
            self.temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.temp_sock.bind(("0.0.0.0", 65533))
            self.temp_sock.settimeout(0.5)

    def run(self):
        self.log.debug("Starting " + self.name)
        while True:
            data = None
            try:
                data, addr = self.temp_sock.recvfrom(1024)
            except socket.timeout:
                pass

            if data:
#                self.log.debug("Got some TEMP data")
                try:
                    msg = json.loads(data)
                    self.temp_current = msg
                    # TODO wysyłać tylko, jeśli temperatura uległa zmianie, teraz zawsze
                    self.tempReader2mqttPublisher.put({"t":"temperature", "v": self.temp_current})
                except:
                    self.log.debug("Parse error during TEMP data processing")

            # Czyta parametry SNMP w rytm odczytu z tempstreamer.tcl
            # TODO temperature tez czytac przez SNMP. Zrezygnowac z tempstramera.
            snmp = self.readOids()
            self.tempReader2mqttPublisher.put({"t":"snmp", "v": snmp})

            if self.exitFlag:
                break
            time.sleep(self.tick)

        self.log.debug("Exiting " + self.name)

    # Tutaj przepytujemy wszystkie OID z config.py. Zwracamy Dict {"key": "value"}
    def readOids(self):
#        self.log.debug("readOids() called")
        values = {}
        for key in self.config["oids"]:
            values[key] = self.readSnmp(self.config["oids"][key])
#        self.log.debug(values)
        return values

    # Tutaj doczytujemy konkretny OID, zwracamy to co przeczytaliśmy
    def readSnmp(self,oid):
#        self.log.debug("readSnmp() called")
        cmdGen = cmdgen.CommandGenerator()
        errorIndication, errorStatus, errorIndex, varBinds = cmdGen.getCmd(
            cmdgen.CommunityData(self.config["community"]),
            cmdgen.UdpTransportTarget((self.config["gw"], 161)),
            oid
        )
#        self.log.debug(varBinds[0])
        return str(varBinds[0][1])
