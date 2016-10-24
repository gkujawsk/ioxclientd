#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pymodbus.datastore import ModbusSlaveContext, ModbusServerContext
from pymodbus.datastore import ModbusSequentialDataBlock
from pymodbus.device import ModbusDeviceIdentification
from pymodbus.transaction import ModbusSocketFramer
from pymodbus.server.sync import ModbusTcpServer
import threading
import logging
import time
from Queue import Queue
logging.getLogger("pymodbus.datastore.context").setLevel(logging.WARNING)
logging.getLogger("pymodbus.transaction").setLevel(logging.WARNING)
logging.getLogger("pymodbus.factory").setLevel(logging.WARNING)
logging.getLogger("pymodbus.server.sync").setLevel(logging.WARNING)


class modbusTcp(threading.Thread):
    def __init__(self, threadID, name, config, modbusRs2modbusTcp, modbusTcp2modbusRs):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.config = config
        self.log = logging.getLogger(name)
        self.modbusRs2modbusTcp = modbusRs2modbusTcp  # kolejka do odbierania danych z modbusRs
        self.modbusTcp2modbusRs = modbusTcp2modbusRs  # kolejka do wysylania danych do modbusRS
        self.tick = 0.01
        self.interval = 0.1
        self.exitFlag = False
        self.store = ModbusSlaveContext(  # Tworzymy rejestr. Inicjalizujemy wszystko zerami.
            di=ModbusSequentialDataBlock(0, [00] * 1),
            co=ModbusSequentialDataBlock(0, [00] * 1),
            hr=ModbusSequentialDataBlock(0, [00] * 100000),
            ir=ModbusSequentialDataBlock(0, [00] * 1))
        # Multipleksacja na podstawie adresów rejestrów HR a nie na poziomie Unit Id.
        self.context = ModbusServerContext(slaves=self.store, single=True)
        self.identity = ModbusDeviceIdentification()
        self.identity.VendorName = 'pymodbus'
        self.identity.ProductCode = 'PM'
        self.identity.VendorUrl = 'itcloud'
        self.identity.ProductName = 'pymodbus Server'
        self.identity.ModelName = 'pymodbus Server'
        self.identity.MajorMinorRevision = '1.0'
        self.framer = ModbusSocketFramer
        self.server = ModbusTcpServer(self.context, self.framer, self.identity,
                                      (self.config["modbusTcp"]["bindIp"], self.config["modbusTcp"]["bindPort"]))

    def _serve(self):
        self.server.serve_forever()
        self.log.debug("FINISHED SERVING")

    def _close(self):
        self.server.shutdown()
        self.server.server_close()
        self.log.debug("Closing MODBUS TCP socket")

    def findAddress(self, address):
        for i in self.config["modbusRs"]["slaves"]["serial1"]:
            if i["address"] == address:
                return i
        return False

    def proccessPdu(self):
        # PDU FORMAT pdu = {"a":SLAVE ADDRESS,"r":REGISTER NAME,"v":REGISTER VALUE}
        # modbusRs puts the array of PDUs on queue
        # Format 0xAABB - AA (0-255) adres MODBUS BB - register (0-254)
        # Only Serial1 is relayed !!!
        if not self.modbusRs2modbusTcp.empty():
            pdus = self.modbusRs2modbusTcp.get()
            for p in pdus:
                slaveId = p["a"] << 8
                r = self.findAddress(p["a"])
                try:
                    regAddress = r["registers"][p["r"]]["address"]
                except KeyError:
                    pass
                else:
                    fullRegAddress = slaveId + regAddress
                    self.context[0].setValues(3, fullRegAddress, [p["v"]])

    def run(self):
        self.log.info("Starting " + self.name)
        t = threading.Thread(target=self._serve)  # wystartuj MODBUS TCP SERVER
        t.name = "MODBUS TCP SERVER"  # chodzi w tle jako wątek
        t.start()  # serwuje dane z rejestrów przygotowanych w konstruktorze
        counter = 0
        while True:  # Kręć w kółko
            if counter * self.tick >= self.interval:
                self.proccessPdu()
                counter = 0
            if self.exitFlag:  # Ale patrz czy czasem mama nie woła do domu
                self._close()  # Pamiętaj, żeby wyłączyć modbus tcp server, bo zostanie sierotą
                break  # Wyskocz z pętli. Zakończ wątek.
            counter += 1
            time.sleep(self.tick)

        self.log.debug("Exiting " + self.name)  # Daj znac, ze watek zakończył pracę
