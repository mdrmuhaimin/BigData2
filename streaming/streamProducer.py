import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import glob, os
import pickle
from streamz import Stream
from streamz.dataframe import Random
from kafka import *
from random import randint
import datetime, time, random, threading
import sys
from xlrd import open_workbook

files_path = '/home/jemd/Documents/SFU/Spring2018/BigData/finalProject/IMUDataset'    # Pass the IMU Dataset directory path
folders = ['ADLs', 'Falls', 'Near_Falls']

def producer(subject) :
    rand = random.Random()
    interval = 0.0008                                    # Messages every 5 secs
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
                msg += folder    							# Add trial type
                outf = open('./streamdata.csv','a+')                          
                outf.write(msg+'\n')
                outf.close()
                time.sleep(interval)
        return # DELETE THIS IF YOU WANT MORE TRIALS. FOR NOW, IT STOPS AFTER 1 TRIAL IS COMPLETE

producer(10)