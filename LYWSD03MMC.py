#!/usr/bin/env python3
from bluepy import btle
import struct 
import time
import logging
import sqlite3
import traceback
import datetime

logging.basicConfig(format='[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s',
                    level=logging.DEBUG)

mac = ''
uuid = 'EBE0CCC1-7A0A-4B0C-8A1A-6FF2997DA3A6'
logger=logging.getLogger("LYWSD03MMC")
logger.debug("start")
conn = sqlite3.connect('LYWSD03MMC.db')
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS hydrothermograph
             (time timestamp, temperature real, humidity integer, batteryLevel real)''')
c.execute('''CREATE INDEX IF NOT EXISTS index_time ON hydrothermograph(time)''')
conn.commit()


class MyDelegate(btle.DefaultDelegate):
    def handleNotification(self, cHandle, data):
        humidity = data[2]
        voltage = struct.unpack('H', data[3:5])[0]/ 1000
        batteryLevel = round((voltage - 2.1),2) * 100
        temperature = struct.unpack('H', data[:2])[0] / 100
        now = datetime.datetime.now()
        c.execute("INSERT INTO hydrothermograph VALUES (?,?,?,?)",(now,temperature,humidity,batteryLevel))
        conn.commit()
        logger.debug("humidity=%s,temperature=%s,batteryLevel=%s",humidity,temperature,batteryLevel)


while True:
    try:
        p = btle.Peripheral(mac)
        p.setDelegate(MyDelegate())
        ch = p.getCharacteristics(uuid=uuid)[0]
        desc = ch.getDescriptors(forUUID=0x2902)[0]
        desc.write(0x01.to_bytes(2, byteorder="little"), withResponse=True)
        p.waitForNotifications(1.0)
        p.disconnect()
        time.sleep(30)
    except KeyboardInterrupt:
        logger.debug("exit")
        conn.close()
        p.disconnect()
        break

    except Exception as e:
        time.sleep(10)
        logger.debug(e)
        logger.debug(traceback.format_exc())
