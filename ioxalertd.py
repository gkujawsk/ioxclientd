#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, sys, time, signal, getopt, traceback
import logging
from daemonize import Daemonize
from Queue import Queue
from threading import Thread
from updater import updater
from config import configuration
from alertSubscriber import alertSubscriber
import constants

threads = []
pid = "/tmp/ioxalertd.pid"
foreground = False
log = logging.getLogger()
log.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('/tmp/ioxalertd.log')
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


class IoxDaemon(Daemonize):
    def start(self):
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
    log.info("ioxalertd version " + constants.VERSION + " started")

    #threads = []

    if "alertSubscriber" in configuration:
        thread = alertSubscriber(4, "alertSubscriber", configuration)
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
    opts, args = getopt.getopt(sys.argv[1:], "hf", ["foreground", "help"])
except getopt.GetoptError:
    print('ioxfeederd.py -h')
    sys.exit(2)

for opt, arg in opts:
    if opt in ("-h", "--help"):
        print('ioxalertd.py -f -h')
        sys.exit()
    elif opt in ("-f", "--foreground"):
        foreground = True

daemon = IoxDaemon(app="ioxalertd", pid=pid, action=main, chdir="./", logger=log, keep_fds=[fh.stream.fileno()],
                   foreground=foreground)
daemon.start()
