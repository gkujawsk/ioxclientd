# -*- coding: utf-8 -*-
import json,logging,time,sys
import threading
import constants
import config
import socket
import pynmea2
from Queue import Queue

class gpsReader (threading.Thread):
    def __init__(self, threadID, name, config, gpsReader2mqttPublisher):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.config = config
        self.log = logging.getLogger("gpsReader")
        self.gps = True
        self.gps_sock = None
        self.gps_position = None
        self.gps_acquired = False
        self.gpsReader2mqttPublisher = gpsReader2mqttPublisher
        self.tick = 0.01
        self.interval = 0.5
        self.exitFlag = False
        if self.gps:
            self.gps_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.gps_sock.bind(("0.0.0.0", 65534))
            self.gps_sock.settimeout(0.5)

    def run(self):
        self.log.debug("Starting " + self.name)
        while True:
            data = None
            try:
                data, addr = self.gps_sock.recvfrom(1024)
            except socket.timeout:
                pass

            if data:
#                self.log.debug("Got some NMEA data")
                try:
                    msg = pynmea2.parse(data)
                    if not self.gps_acquired: # Pozycja jeszcze nie ustalona
                        #self.log.info("GPS position acquired successfully.")
                        self.gps_position = {"altitude":format(msg.altitude,'.2f'),
                                             "lat":format(msg.latitude,".5f"),
                                             "latitude":'%02d°%02d′%07.4f″' % (msg.latitude, msg.latitude_minutes, msg.latitude_seconds),
                                             "lat_dir":msg.lat_dir,
                                             "lon":format(msg.longitude,".5f"),
                                             "longitude":'%02d°%02d′%07.4f″' % (msg.longitude, msg.longitude_minutes, msg.longitude_seconds),
                                             "lon_dir":msg.lon_dir}
                        # TODO wysyłać tylko, jeśli pozycja uległa zmianie, teraz zawsze
                        self.gpsReader2mqttPublisher.put({"t": "position", "v": self.gps_position})
                        self.gps_acquired = True
                    else: # Pozycja ustalona. Wyślij tylko jak ulegnie zmianie.
                        if self.gps_position["lat"] != format(msg.latitude,".5f") or self.gps_position["lon"] != format(msg.longitude,".5f"):
                            #self.log.info("GPS position changed.")
                            self.gps_position = {"altitude":format(msg.altitude,'.2f'),
                                                 "lat":format(msg.latitude,".5f"),
                                                 "latitude":'%02d°%02d′%07.4f″' % (msg.latitude, msg.latitude_minutes, msg.latitude_seconds),
                                                 "lat_dir":msg.lat_dir,
                                                 "lon":format(msg.longitude,".5f"),
                                                 "longitude":'%02d°%02d′%07.4f″' % (msg.longitude, msg.longitude_minutes, msg.longitude_seconds),
                                                 "lon_dir":msg.lon_dir}
                            # TODO wysyłać tylko, jeśli pozycja uległa zmianie, teraz zawsze
                            self.gpsReader2mqttPublisher.put({"t":"position", "v": self.gps_position})
                except Exception as e:
               #     self.log.debug(e.message)
                    pass
#                    self.log.debug("Parse error during NMEA data processing")

            if self.exitFlag:
                break

            time.sleep(self.tick)

        self.log.debug("Exiting " + self.name)
