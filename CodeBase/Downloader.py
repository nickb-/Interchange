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
    
    def __init__(self, source):
        # connect the api to source (default = poloniex)
        with open("../Secrets/secrets.json") as f:
            secrets = json.load(f)    
            
        self.api = poloniex(secrets[source]['key'], secrets[source]['secret'])
        self.data_directory = "../Data/"
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
            elif type == float or type == int:    # i.e. unix timestamp
                return datetime.datetime.utcfromtimestamp(datestamp).strftime(format)
            else:
                print("Invalid datestamp")
        else:
            pass
        
    def download(self, method = 'refresh', date_range = {'start_date': '2018-01-01 00:00:00',
                                                         'end_date': '2018-01-01 01:00:00'}):
        if method == 'refresh':
            # Download all available tickers from start of 2018 to now
            date_range['end_date'] = self.parse_date(datetime.datetime.now(), output = 'utc')
            destination = "{}/{}".format(self.data_directory, self.source)
            tickers = self.api.returnTicker()
            
            # check if Source data directory already exists
            if os.path.exists(destination):
                for existing in os.listdir(destination):
                    os.remove("{}/{}".format(destination, existing))
            else:
                os.mkdir(destination)
                
            # Now, iterate over the tickers and save to CSV files
            for currency in tickers.keys():
                data = self.api.returnChartData(currency, date_range['start_date'], date_range['end_date'])
                data = pd.io.json.json_normalize(data)
                
                filename = "PoloniexChartData_{}.csv".format(currency)
                data.to_csv("{}/{}".format(destination, filename), index = False)
                
                
        else:
            pass

        
if __name__ == '__main__':
    
    dw = Downloader('poloniex')
    dw.download(method = 'refresh')