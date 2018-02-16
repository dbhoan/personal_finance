# -*- coding: utf-8 -*-
# Author: Hoan Bui Dang
# Python: 2.7

"""
Functions for USD/CAD exchange rates.
"""

import numpy as np
import pandas as pd
import datetime as dt
import os
import urllib # for downloading
import csv # for reading csv files


rate_folder = 'fxrate/'
rate_filename = 'fxrates.csv'
    
# check if available rates are up to date
current_data = pd.read_csv(rate_filename)
last_date = pd.to_datetime(current_data.Date).iloc[-1].to_pydatetime()
now = dt.datetime.now()
delta = now - last_date
if delta.days >= 1:
    print 'Latest exchange rate was', delta.days, 'days old.'
    print 'Updating rate data from Bank of Canada...'
    from_date = last_date + dt.timedelta(days=1)
    url = 'https://www.bankofcanada.ca/valet/observations/FXUSDCAD/csv?'\
            + 'start_date=' + from_date.strftime('%Y-%m-%d')\
            + '&end_date=' + now.strftime('%Y-%m-%d')
    filename = from_date.strftime('%Y%m%d') + '-' + now.strftime('%Y%m%d') + '.csv'
    if not os.path.exists(rate_folder): os.makedirs(rate_folder)
    response = urllib.urlretrieve(url,rate_folder + filename)
    new_data = pd.read_csv(rate_folder + filename, skiprows=8)
    temp = pd.DataFrame()
    temp['Date'] = new_data['date']
    temp['USD'] = new_data['FXUSDCAD']
    rate = pd.concat([current_data, temp])
    rate.to_csv(rate_filename, index=False)
    print 'Rate data have been successfully updated.'
else:
    print 'Exchange rates in ' + rate_filename + ' are up to date.'
    rate = current_data
rate['Date'] = pd.to_datetime(rate['Date'])


