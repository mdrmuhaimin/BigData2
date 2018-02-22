from kafka import KafkaProducer
from random import randint
import datetime, time, random, threading
import glob, os
import pickle
import sys
from xlrd import open_workbook

subjects = [6] # messages per second

files_path = sys.argv[1]    # Pass the IMU Dataset directory path
files = glob.glob(files_path)
folders = ['ADLs', 'Falls', 'Near_Falls']

def send_at(subject):
    rand = random.Random()
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(0,10))
    topic = 'trials-' + str(subject)                        # Name the topic "trials-(numSubject)"
    interval = 5                                            # Messages every 5 secs

    while True :
        folder = folders[randint(0,2)]                      # Pick a random folder
        path = files_path+'/sub'+str(subject)+'/'+str(folder)+'/*.xlsx'
        files = glob.glob(path)
        f = files[randint(0,len(files)-1)]                  # Pick a random trial

        wb = open_workbook(f)                   
        for sheet in wb.sheets():                           # Iterate over the excel document
            number_of_rows = sheet.nrows
            number_of_columns = sheet.ncols
            for row in range(1, number_of_rows):
                values = []
                for col in range(number_of_columns):
                    value  = (sheet.cell(row,col).value)
                    value = str(float(value))
                    values.append(value)

                msg = ''
                for v in values :                           # Add all the values
                    msg += str(v) + ","
                msg += f + ","                              # Add filename
                msg += str(subject) + ","                   # Add subject
                msg += folder                               # Add trial type
                producer.send(topic,msg.encode('ascii'))    # Send the message

                time.sleep(interval)
        
if __name__ == "__main__":
    for sub in subjects:
        server_thread = threading.Thread(target=send_at, args=(sub,))
        server_thread.setDaemon(True)
        server_thread.start()

    while 1:
        time.sleep(1)
