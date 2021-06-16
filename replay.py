import os
import sys
import csv
import time
import datetime
import logging
from threading import Thread

logger_format = '%(asctime)s:%(threadName)s:%(message)s'
logging.basicConfig(format=logger_format, level=logging.INFO, datefmt="%H:%M:%S")

if len(sys.argv) != 2:
    print("Usage:", sys.argv[0], "<path_to_csv_files>")
    exit(0)

base_dir = sys.argv[1]

class Event():
    def __init__(self, key, value, timestamp):
        self.key = key
        self.value = value
        self.timestamp = timestamp

    def __str__(self):
        return self.key + ": " + str(self.timestamp) + " - " + self.value

def sensor(filename):
    logging.info(f"{filename} started")
    #start = datetime.datetime.now() # TODO: take starting point as an argument later
    with open(base_dir + filename, "r") as csv_file:
        csv_reader = csv.reader(csv_file)
        headers = next(csv_reader)
        key = os.path.basename(filename)[0:-4] # filename will be used as key
        previous_ts = None
        count = 0
        for row in csv_reader:
            if len(row[0]) == 26: # timestamp with milliseconds
                ts = datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f")
            elif len(row[0]) == 19: # timestamp without milliseconds
                ts = datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
            else:
                logging.waring(f"Invalid timestamp: {row[0]}")
                continue
            
            v = row[1]
            e = Event(key, v, ts)
            
            if previous_ts != None:
                delta = ts - previous_ts
                time.sleep(delta.total_seconds())

            logging.info(e)
            previous_ts = ts
            count += 1
            if count >= 3:
                break
    
    logging.info(f"{filename} finished")

def main():
    logging.info("Main started")
    threads = [Thread(target=sensor, args=(file, )) for file in os.listdir(path=base_dir)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join() # waits for thread to complete its task
    logging.info("Main Ended")

# go
main()
