#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, sys, time, signal, getopt, traceback
import logging
from daemonize import Daemonize
from Queue import Queue
from threading import Thread
from modbusRs import modbusRs
from mqttPublisher import mqttPublisher
from mqttSubscriber import mqttSubscriber
#from mqttInfluxdb import mqttInfluxdb
from updater import updater
from gpsReader import gpsReader
from tempReader import tempReader
from modbusTcp import modbusTcp
from config import configuration
import constants
from pqueue import PersistentQueue

threads = []
pid = "/tmp/ioxclientd.pid"
foreground = False
log = logging.getLogger()
log.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('/tmp/ioxclientd.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - (%(threadName)-10s) - %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)
# add the handlers to logger
log.addHandler(ch)
log.addHandler(fh)


class IoxDaemon (Daemonize):
    def start(self):
        self.logger.debug("Start overriden by ioxDaemon class. Setting SIGQUIT handler.")
        signal.signal(signal.SIGQUIT, self.dumpstacks)
        super(IoxDaemon, self).start()

    def sigterm(self, signum, frame):
        self.logger.warn("Caught signal %s. Stopping daemon." % signum)
        for t in threads:
            t.exitFlag = True
        sys.exit(0)

    def dumpstacks(self, signum, frame):
        self.logger.warn("Caught signal %s. Dumping stack trace." % signum)
        id2name = dict((th.ident, th.name) for th in threading.enumerate())
        for threadId, stack in sys._current_frames().items():
            print(id2name[threadId])
            traceback.print_stack(f=stack)


def main():
    global log
    log.info("ioxclientd version "+ constants.VERSION +" started")
    modbusRs2mqttPublisher = Queue()
    #modbusRs2mqttPublisher = PersistentQueue("/tmp/modbusRs2mqttPublisher")
    if modbusRs2mqttPublisher.qsize() > 0:
        log.warn("Trying to recover (%d) items from modbusRs2mqttPublisher queue." % modbusRs2mqttPublisher.qsize())
    mqttSubscriber2modbusRs = Queue()
    modbusRs2modbusTcp = Queue()
    modbusTcp2modbusRs = Queue()
    mqttSubscriber2updater = Queue()
    updater2mqttPublisher = Queue()
    #gpsReader2mqttPublisher = Queue()
    gpsReader2mqttPublisher = PersistentQueue("/tmp/gpsReader2mqttPublisher")
    if gpsReader2mqttPublisher.qsize() > 0:
        log.warn("Trying to recover (%d) items from gpsReader2mqttPublisher queue." % gpsReader2mqttPublisher.qsize())
    #tempReader2mqttPublisher = Queue()
    tempReader2mqttPublisher = PersistentQueue("/tmp/tempReader2mqttPublisher")
    if tempReader2mqttPublisher.qsize() > 0:
        log.warn("Trying to recover (%d) items from tempReader2mqttPublisher queue." % tempReader2mqttPublisher.qsize())

    threads = []

    # Wystartuj serialowego czytacza modbusa
    if("modbusRs" in configuration["modules"]):
        thread = modbusRs(1, "modbusRs", configuration["modbusRs"],
                      modbusRs2mqttPublisher, mqttSubscriber2modbusRs, modbusRs2modbusTcp, modbusTcp2modbusRs)
        thread.start()
        threads.append(thread)
    if("modbusTcp" in configuration["modules"]):
        thread = modbusTcp(2,"modbusTcp", configuration,
                       modbusRs2modbusTcp,modbusTcp2modbusRs)
        thread.start()
        threads.append(thread)

    if("mqttPublisher" in configuration["modules"]):
        thread = mqttPublisher(3, "mqttPublisher", configuration["mqttPublisher"],
                           modbusRs2mqttPublisher,gpsReader2mqttPublisher,tempReader2mqttPublisher)
        thread.start()
        threads.append(thread)

    if("mqttSubscriber" in configuration["modules"]):
        thread = mqttSubscriber(4, "mqttSubscriber", configuration["mqttSubscriber"],
                            mqttSubscriber2modbusRs, mqttSubscriber2updater)
        thread.start()
        threads.append(thread)

#    if("mqttInfluxdb" in configuration["modules"]):
#        thread = mqttInfluxdb(5,"mqttInfluxdb",configuration["mqttInfluxdb"])
#        thread.start()
#        threads.append(thread)

    if("updater" in configuration["modules"]):
        thread = updater(5,"updater", configuration["updater"], mqttSubscriber2updater, updater2mqttPublisher)
        thread.start()
        threads.append(thread)

    if("gpsReader" in configuration["modules"]):
        thread = gpsReader(6, "gpsReader", configuration["gpsReader"], gpsReader2mqttPublisher)
        thread.start()
        threads.append(thread)

    if("tempReader" in configuration["modules"]):
        thread = tempReader(7, "tempReader", configuration["tempReader"], tempReader2mqttPublisher)
        thread.start()
        threads.append(thread)

    while len(threads) > 0:
        try:
            # Join all threads using a timeout so it doesn't block
            # Filter out threads which have been joined or are None
            for t in threads:
                if t is not None and t.isAlive():
                    t.join(1)
        except KeyboardInterrupt:
            log.info("Ctrl-c received! Sending kill to threads...")
            for t in threads:
                t.exitFlag = True
            exit(2)

try:
    opts, args = getopt.getopt(sys.argv[1:],"hf",["foreground","help"])
except getopt.GetoptError:
    print('ioxclientd.py -h')
    sys.exit(2)

for opt, arg in opts:
  if opt in ("-h", "--help"):
    print('ioxclientd.py -f -h')
    sys.exit()
  elif opt in ("-f", "--foreground"):
    foreground = True

daemon = IoxDaemon(app="ioxclientd", pid=pid, action=main, chdir="./", logger=log, keep_fds=[fh.stream.fileno()], foreground=foreground)
daemon.start()

#if __name__ == '__main__':
#  main(sys.argv)









