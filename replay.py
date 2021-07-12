import os
import sys
import csv
import time
import json
import datetime
import logging
from threading import Thread
from paho.mqtt import client as mqtt_client

logger_format = '%(asctime)s:%(threadName)s:%(message)s'
logging.basicConfig(format=logger_format, level=logging.DEBUG, datefmt="%H:%M:%S")

# MQTT config
mqtt_broker = os.getenv('MQTT_BROKER_ADDRESS', 'localhost')
mqtt_port = os.getenv('MQTT_BROKER_PORT', 1883)
#mqtt_username = 'emqx'
#mqtt_password = 'public'

# data config 
data_dir = os.getenv('DATA_DIRECTORY', 'data/') # folder with CSV data files (with trailing slash)
starting_time = datetime.datetime(2019, 4, 1) # must be set to a time before any sensored data items

# connects to MQTT broker
def connect_mqtt(client_id):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to MQTT Broker!")
        else:
            logging.info("Failed to connect, return code %d\n", rc)
    
    # set connecting client ID
    client = mqtt_client.Client(client_id)
    #client.username_pw_set(mqtt_username, mqtt_password)
    client.on_connect = on_connect
    client.connect(mqtt_broker, mqtt_port)
    return client

# publishes sensored data from file to MQTT broker
def sensor(filename):
    # filename will be used as key/topic
    key = os.path.basename(filename)[0:-4] 
    logging.info(f"sensor {key} started")
    
    #start = datetime.datetime.now()
    # read data from CSV datafile
    with open(data_dir + filename, "r") as csv_file:
        csv_reader = csv.reader(csv_file)
        headers = next(csv_reader)
        if len(headers) != 2: # expected exactly two columns (timestamp, value)
            logging.warning("Invalid CSV file format!")
            return

        # connect to MQTT broker
        client = connect_mqtt(key)
        client.loop_start()

        # set first "previous" timestamp
        previous_ts = starting_time
        count = 0
        topic = "/sensor/" + key
        # every row will be sent as an 'event'
        for row in csv_reader:
            if len(row[0]) == 26: # timestamp with milliseconds
                ts = datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f")
            elif len(row[0]) == 19: # timestamp without milliseconds
                ts = datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
            else:
                logging.waring(f"Invalid timestamp: {row[0]}")
                continue # next row
            
            # the value should be usually a float
            try:
                value = float(row[1])
            except:
                value = row[1] # some other type will go as string
            # json encoded timestamp (row[0]), value (row[1]) pair
            isotime = ts.isoformat()
            msg = json.dumps({headers[0]: isotime, headers[1]: value})
            
            # sleeps an accorging to the interval between events/rows
            delta = ts - previous_ts
            # sleeps just before sending the next event
            sleep = delta.total_seconds()
            logging.debug(f"Sleeping for {sleep} seconds...")
            time.sleep(sleep)

            result = client.publish(topic, msg)
            count += 1
            # result: [0, 1]
            status = result[0]
            if status == 0: # success
                logging.info(f"Sent `{msg}` on topic `{topic}` (count {count})")
            else: # failure
                logging.warning(f"Failed to send message `{msg}` on topic {topic} (count {count})")
            
            # update previous timestamp
            previous_ts = ts
            #if count >= 3: # break after N messages
            #    break # temporary
    
    logging.info(f"sensor {key} finished")

# start all sensor threads
def main():
    logging.info("Main started")
    # every sensor will run as a separate thread
    threads = [Thread(target=sensor, args=(filename, )) for filename in os.listdir(path=data_dir)]
    for thread in threads:
        thread.start() # start every sensor
    for thread in threads:
        thread.join() # waits for sensor to finish sending data
    logging.info("Main Ended")

# go
main()
