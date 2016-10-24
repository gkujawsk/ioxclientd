# -*- coding: utf-8 -*-
from Queue import Queue

import threading
import logging
import time
import datetime
import serial
import minimalmodbus
import struct
from math import sin
from math import asin


def calculateCapacity(p):
    r = 7.95  # promien wewnętrzny
    g = 10.0  # przyspieszenie ziemskie
    ro = 860  # gestosc cieczy
    l = 50.366  # dlugosc wewnetrzna
    h = p / (g * ro) * 10  # wysokosc slupa cieczy
    v = float(int(0.5 * (
    3.14 - 2 * asin((r - h) / r) - sin(3.14 - 2 * asin((r - h) / r))) * r * r * l))  # zaokrąglona pojemnosc w litrach
    return v


def calculateTempPt10050150(p):
    # wartość 16 bitowa, najstarszy bit, to bit znaku, gdzie 1 to minus, a 0 to plus
    v = int((((float(p)/65535) * (150 + 50)) - 50)*100)  # zakres -50 do 150. Odczyt z dokładnością do 2 miejsc po przecinku.
    v = v if v >= 0 else (1 << 16) + v  # jeśli wartość jest na minusie, to dodaj bit znaku.
    return v

def oneDecimal(p):
    return p / 10


class modbusRs(threading.Thread):
    def __init__(self, threadID, name, config, modbusRs2mqttPublisher, mqttSubscriber2modbusRs,
                 modbusRs2modbusTcp, modbusTcp2modbusRs):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.config = config
        self.log = logging.getLogger("modbusRs")
        self.modbusRs2mqttPublisher = modbusRs2mqttPublisher
        self.mqttSubscriber2modbusRs = mqttSubscriber2modbusRs
        self.modbusRs2modbusTcp = modbusRs2modbusTcp
        self.modbusTcp2modbusRs = modbusTcp2modbusRs
        self.tick = 0.01
        self.interval = 3
        self.exitFlag = False
        self.s = {
            "serial1": minimalmodbus.Instrument(self.config["serial1"]["port"], 1),
            "serial2": minimalmodbus.Instrument(self.config["serial2"]["port"], 1)}

        self.s["serial1"].serial.baudrate = self.config["serial1"]["speed"]
        self.s["serial1"].serial.bytesize = self.config["serial1"]["bytesize"]
        self.s["serial1"].serial.stopbits = self.config["serial1"]["stopbits"]
        self.s["serial1"].serial.timeout = self.config["serial1"]["timeout"]
        self.s["serial1"].mode = minimalmodbus.MODE_RTU

        self.s["serial2"].serial.baudrate = self.config["serial2"]["speed"]
        self.s["serial2"].serial.bytesize = self.config["serial2"]["bytesize"]
        self.s["serial2"].serial.stopbits = self.config["serial2"]["stopbits"]
        self.s["serial2"].serial.timeout = self.config["serial2"]["timeout"]
        self.s["serial2"].mode = minimalmodbus.MODE_RTU

    def run(self):
        self.log.info("Starting " + self.name)
        counter = 0
        while True:
            if (counter * self.tick >= self.interval):
                self.readSerial()
                counter = 0
            if self.exitFlag:
                break
            if not self.mqttSubscriber2modbusRs.empty():
                msg = self.mqttSubscriber2modbusRs.get()
                self.log.debug("modbusRs.py: received message: " + msg.topic + " " + msg.payload)
                command = msg.topic.split("/")[2]
                parameter = msg.topic.split("/")[3]
                if command == "set":
                    pass
                elif command == "get":
                    if parameter == "update":
                        self.readSerial()
                    else:
                        self. ing("Unknown get command parameter: %s" % (parameter))
                else:
                    self.log.warning("Unknown command: %s" % (command))
            counter += 1
            time.sleep(self.tick)
        self.log.debug("Exiting " + self.name)

    def readSerial(self):
        # self.log.debug("readSerial() called")
        # czytamy tylko serial1
        for serial in self.config["slaves"]:
            for slave in self.config["slaves"][serial]:
                self.s[serial].address = slave["address"]
                pdus = []
                for r in slave["registers"]:
                    try:
                        value = ""
                        type = slave["registers"][r]["type"]
                        self.s[serial].serial.read(100)  # sprawdź czy nie ma czasem śmieci i dopiero czytaj dalej
                        if not type:
                            value = self.s[serial].read_register(slave["registers"][r]["address"], 0, 4)
                        else:
                            if type == "string":
                                value = self.s[serial].read_string(slave["registers"][r]["address"],
                                                                   slave["registers"][r]["param"]).encode("hex")
                            elif type == "long":  # czytaj dwa rejestry (32-bit) jako long
                                value = self.s[serial].read_long(slave["registers"][r]["address"])
                            elif type == "float":  # czytaj dwa rejestry (32-bit) jako float
                                value = self.s[serial].read_float(slave["registers"][r]["address"])
                            elif type == "bit":  # czytaj coil (1-bit)
                                value = self.s[serial].read_bit(slave["registers"][r]["address"])
                            elif type == "register":
                                if "param" in slave["registers"][r]:
                                    param = slave["registers"][r]["param"]
                                else:
                                    param = 0
                                if "code" in slave["registers"][r]:
                                    code = slave["registers"][r]["code"]
                                else:
                                    code = 4
                                value = self.s[serial].read_register(slave["registers"][r]["address"], param, code)
                            elif type == "registers":
                                if "param" in slave["registers"][r]:
                                    param = slave["registers"][r]["param"]
                                else:
                                    param = 0
                                value = self.s[serial].read_registers(slave["registers"][r]["address"], param)

                        if "when" in slave["registers"][r]:  # emituj tylko gdy
                             if slave["registers"][r]["when"] == "onChange":  # zmieniła się wartość odczytu
                                 if "value" in slave["registers"][r]:  # czy kiedys juz był odczyt?
                                     if slave["registers"][r]["value"] == value: # poprzednim razem było to samo
                                         continue # nie wysyłaj, czytaj kolejny rejestr
                                     else: # nowy odczyt, zapisz do porównań w następnej rundzie odczytów
                                         slave["registers"][r]["value"] = value
                                 else:  # ten rejestr jeszcze nigdy nie byl odczytywany, zapis wartosc na przyszlosc
                                     slave["registers"][r]["value"] = value

                        if "callback" in slave["registers"][r]:
                            value = eval(slave["registers"][r]["callback"] + "(" + str(value) + ")")

                        if "override" in slave["registers"][r]:
                            reg = slave["registers"][r]["override"]
                        else:
                            reg = r
                        pdus.append({"a": slave["address"], "r": reg, "v": value, "t": "sensor",
                                     "timestamp": int(datetime.datetime.today().strftime('%s'))})
                    except IOError as e:
                        pdus.append({"a": slave["address"], "r": "ERR", "v": "N/A", "t": "sensor",
                                     "timestamp": int(datetime.datetime.today().strftime('%s'))})
                        self.log.warn("Error during reading modbus slave (%d) (%s)" % (slave["address"], e.message))
                    except ValueError as e:
                        self.log.warn("Error during reading modbus slave (%d) (%s)" % (slave["address"], e.message))
                        # Spróbuj odczytac 10000 znakow. Moze to przeczyscie smietnik.
                        self.s[serial].serial.read(1000)

                    #            self.log.debug(pdus)
                self.modbusRs2mqttPublisher.put(pdus)
                self.log.debug("QUEUE SIZE: %d" % self.modbusRs2mqttPublisher.qsize())
                #  self.log.debug("CURRENT QUEUE COUNT: (%d)" % self.modbusRs2mqttPublisher.qsize())
                self.modbusRs2modbusTcp.put(pdus)
