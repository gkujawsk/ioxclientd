# -*- coding: utf-8 -*-
import requests, json, logging, hashlib, os, glob, shutil, tarfile,time,sys,psutil
import threading
import constants
from Queue import Queue
requests.packages.urllib3.disable_warnings()
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

class updater (threading.Thread):
    def __init__(self, threadID, name, config, mqttSubscriber2updater, updater2mqttPublisher):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.config = config
        self.log = logging.getLogger("updater")
        self.mqttSubscriber2updater = mqttSubscriber2updater
        self.updater2mqttPublisher = updater2mqttPublisher
        self.tick = 0.01
        self.interval = 0.5
        self.exitFlag = False

        self.url = self.config["url"]
        self.manifest = self.config["manifest"]
        self.cert = self.config["cert"]
        self.key = self.config["key"]

        self.expected_mime = self.config["expected_mime"]
        self.tmp_dir = self.config["tmp_dir"]
        self.install_dir = self.config["install_dir"]
        self.filename = ""

    def download(self):
        global VERSION
        m = {}
        self.log.debug("download() called")
        r = requests.get(self.url+self.manifest, verify=False, cert=(self.cert,self.key))
        try:
            m = json.loads(r.content)
        except ValueError as e:
            self.log.warn("Unable to parse manifest due to malformed format (%s) " %str(e))
            exit(2)

        if m["stable"] == constants.VERSION:
            raise ValueError('Already running version %s. Not going to update.' % (constants.VERSION))

        self.log.debug("Got new version %s. Going to update." % (m["stable"]))

        self.filename = "ioxclientd."+m["stable"]+".tar.gz"
        r = requests.get(self.url+self.filename, verify=False, cert=(self.cert,self.key),stream=True)
        with open(self.tmp_dir + self.filename, "wb") as file:
            file.write(r.content)

        r = requests.get(self.url+self.filename+".checksum", verify=False, cert=(self.cert,self.key))
        oSha512 = r.content.split()[0]
        dSha512 = hashlib.sha512(open(self.tmp_dir + self.filename,'rb').read()).hexdigest()

        if oSha512 == dSha512:
            self.log.info("Update successfuly downloaded. Checksum verified correctly.")
        else:
            self.log.warn("Failed to verify the checksum of downloaded update. Giving up.")
            exit(1)

    def backup(self):
        self.log.debug("backup() called")
        try:
            os.mkdir(self.install_dir + "backup")
        except OSError as e:
            if(e.errno == 17): # to tylko znaczy, że katalog juz istnieje
                pass
            else: # A tutaj coś innego wypadło
                self.log.warn("Error creating backup directory (%s)" %(e.strerror))
                return
        try:
            for pyfile in glob.iglob(os.path.join(self.install_dir, "*.py")):
                shutil.copy(pyfile, self.install_dir+"backup")
        except IOError as e:
            self.log.warn("Error during backup (%s)." %(str(e)))
            return
        except OSError as e:
            self.log.warn("Error during backup (%s)." %(e.strerror))
            return

    def install(self):
        self.log.debug("install() called")
        self.log.info("Preparing to install update.")
        os.chdir(self.install_dir)
        try:
            tar = tarfile.open(self.tmp_dir + self.filename, "r:gz")
            tar.extractall()
            tar.close()
            self.log.info("Update installed successfuly.")
        except IOError as e:
            self.log.warn("Error during file extraction (%s)" % (str(e)))
            self.log.info("Upgrade failed.")

    def restart(self):
        self.log.debug("restart() called")
        os.chdir(self.install_dir)
        self.log.info("Going to restart... (soon)")
        try:
            p = psutil.Process(os.getpid())
            for handler in p.open_files() + p.connections():
                os.close(handler.fd)
        except Exception as e:
            logging.error(e)
        python = sys.executable
        os.remove("/tmp/ioxclientd.pid")
        os.execl(python, python, *sys.argv)

    def run(self):
        self.log.debug("Starting " + self.name)
        while True:
            if not self.mqttSubscriber2updater.empty():
                msg = self.mqttSubscriber2updater.get()
                self.log.debug("Received message: " + msg.topic + " " + msg.payload)
                command = msg.topic.split("/")[3]
                func = msg.topic.split("/")[4]
                if command == "update":
                    if func == "software":
                        self.log.info("Update requested. Processing...")
                        try:
                            self.download()
                            self.backup()
                            self.install()
                            self.restart()
                        except ValueError as e:
                            self.log.info("%s" %(str(e)))
                    elif func == "reload":
                        # TODO To trzeba jakos uwierzytelniac koniecznie
                        self.log.info("Reload requested. Processing...")
                        self.restart()
                    else:
                        self.log.warn("Unknown function: %s " % (func))
                else:
                    self.log.warn("Unknown command: %s" % (command))
            if self.exitFlag:
                break
            time.sleep(self.tick)
        self.log.debug("Exiting " + self.name)

    def processPdu(self):
        self.log.debug("processPdu() called")
