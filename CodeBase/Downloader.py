#! /bin/bash/python3  

# Downloader.py
#
# Download Poloniex chart data
#
# Nick Burns
# April 2018

import pandas as pd
from PoloniexAPI import *
import json
import os
import time
import datetime

class Downloader():
    
    def __init__(self, source, data_directory = '../Data/'):
        # connect the api to source (default = poloniex)
        with open("../Secrets/secrets.json") as f:
            secrets = json.load(f)    
            
        self.api = poloniex(secrets[source]['key'], secrets[source]['secret'])
        self.data_directory = data_directory
        self.source = source
        
    def parse_date(self, datestamp, output = "utc", format = "%Y-%m-%d %H:%M:%S"):
        if output == "unix":
            if type(datestamp) == str:
                return time.mktime(time.strptime(datestamp, format))
            elif type(datestamp) == datetime.datetime:
                return time.mktime(datestamp.timetuple())
            else:
                print("Invalid datestamp")
                
        elif output == "utc":
            if type(datestamp) == datetime.datetime:
                return datestamp.strftime(format)
            elif type(datestamp) == float or type(datestamp) == int:    # i.e. unix timestamp
                return datetime.datetime.utcfromtimestamp(datestamp).strftime(format)
            else:
                print("Invalid datestamp")
        else:
            pass
        
    def download(self, method = 'refresh', date_range = {'start_date': '2018-01-01 00:00:00',
                                                         'end_date': '2018-01-01 01:00:00'}):
        
        tickers = self.api.returnTicker()
        destination = "{}/{}".format(self.data_directory, self.source)
        
        if method == 'refresh':
            
            # check if Source data directory already exists, wipe it and recreate it
            if os.path.exists(destination):
                for existing in os.listdir(destination):
                    os.remove("{}/{}".format(destination, existing))
            else:
                os.mkdir(destination)
             
        for currency in tickers.keys():
            filename = "PoloniexChartData_{}.csv".format(currency)
            file_location = "{}/{}".format(destination, filename)
            
            # download the data if it does not already exist
            if not os.path.exists(file_location):
                data = self.api.returnChartData(currency, date_range['start_date'], date_range['end_date'])
                data = pd.io.json.json_normalize(data)

                data['Datestamp'] = [datetime.datetime.utcfromtimestamp(x).strftime(format = "%Y-%m-%d %H:%M:%S") \
                                         for x in data['date']]
                filename = "PoloniexChartData_{}.csv".format(currency)
                data.to_csv(file_location, index = False)
                   
if __name__ == '__main__':
    
    dw = Downloader('poloniex')
    dw.download(method = 'refresh')