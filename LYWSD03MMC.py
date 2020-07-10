#!/usr/bin/env python3
from bluepy import btle
import struct 
import time
import logging
import sqlite3
import traceback
import datetime
import os,signal
import multiprocessing                                                                                                 
import psutil

logging.basicConfig(format='[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s',
                    level=logging.DEBUG)

mac = ''
uuid = 'EBE0CCC1-7A0A-4B0C-8A1A-6FF2997DA3A6'
logger=logging.getLogger("LYWSD03MMC")
conn = sqlite3.connect('LYWSD03MMC.db')
c = conn.cursor()


class MyDelegate(btle.DefaultDelegate):
    def handleNotification(self, cHandle, data):
        humidity = data[2]
        voltage = struct.unpack('H', data[3:5])[0]/ 1000
        batteryLevel = round((voltage - 2.1),2) * 100
        temperature = struct.unpack('H', data[:2])[0] / 100
        now = datetime.datetime.now()
        c.execute("INSERT INTO hydrothermograph VALUES (?,?,?,?)",(now,temperature,humidity,batteryLevel))
        conn.commit()
        logger.info("humidity=%s,temperature=%s,batteryLevel=%s",humidity,temperature,batteryLevel)

def ble_getdata(mac_address):
    try:
        p = btle.Peripheral(mac_address)
        p.setDelegate(MyDelegate())
        ch = p.getCharacteristics(uuid=uuid)[0]
        desc = ch.getDescriptors(forUUID=0x2902)[0]
        desc.write(0x01.to_bytes(2, byteorder="little"), withResponse=True)
        p.waitForNotifications(5.0)
        time.sleep(1)

    except KeyboardInterrupt:
            raise KeyboardInterrupt
    except Exception as e:
            logger.debug(e)
            logger.debug(traceback.format_exc())
            raise
    finally:
        p.disconnect()
        time.sleep(2)

    

def kill_bluepy(pid):
    procs = psutil.Process(pid).children()
    logger.debug("children:%s",procs)
    for p in procs:
        if p.name() == "bluepy-helper":
            p.kill()
            gone = p.wait()
            logger.debug("bluepy-helper(%s) killed",p.pid)


def bluepy_timeout_killer(pid):
    logger.debug("watchdog start")
    time.sleep(60)
    kill_bluepy(pid)
    logger.info("watchdog end and bluepy timeout")

def main():
    logger.debug("start")
    pid = os.getpid()

    c.execute('''CREATE TABLE IF NOT EXISTS hydrothermograph
             (time timestamp, temperature real, humidity integer, batteryLevel real)''')
    c.execute('''CREATE INDEX IF NOT EXISTS index_time ON hydrothermograph(time)''')
    conn.commit()

    while True:
        try:
            watchdog = multiprocessing.Process(target=bluepy_timeout_killer,args=(pid,))
            watchdog.start()
            logger.debug("bluepy start")
            ble_getdata(mac)
            kill_bluepy(pid)
            logger.debug("bluepy end")
            watchdog.terminate()
            os.wait()
            logger.debug("watchdog terminate")
            time.sleep(300)
        except KeyboardInterrupt:
            logger.debug("exit")
            conn.close()
            watchdog.terminate()
            kill_bluepy(pid)
            break
    
        except Exception as e:
            watchdog.terminate()
            kill_bluepy(pid)
            time.sleep(15)
            logger.debug(e)
            logger.debug(traceback.format_exc())

if __name__ == "__main__":
    main()
