# -*- coding: utf-8 -*-
# Author: Hoan Bui Dang
# Python: 3.6

""" 
A package of tools for investment:
    
    downloadPrice(symbol, folder, start, end, source, varysymbol, trials):
        Download daily price for a single or list of symbols.
    
    loadPrice(symbol, folder=price_folder, start, end): 
        Load price from csv for a single or list of symbols.
        Return a series or a dictionary of series.
    
    downloadFxRate(folder, start, end):
        Downloading and saving exchange rates to csv file.
    
    loadFxRate(folder=fxrate_folder, start='2012-01-03', end=today):
        Load rates from csv file and return a series indexed by dates.
"""

import pandas as pd
import datetime as dt
import os # for creating folders
import io # for reading page content to dataframe
import requests # for downloading from the web

# =============================================================================
# Parameters and global variables
# =============================================================================
today = dt.datetime.now().strftime("%Y-%m-%d")

price_folder = 'price'
fxrate_folder = 'fxrate'
fxrate_file = 'USD.csv'

# API information for downloading daily stock prices
URLs = ['https://query1.finance.yahoo.com/v7/finance/download/var_symbol?period1=var_start&period2=var_end&interval=1d&events=history&crumb=6KMfRzsn26l',
        'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=var_symbol&outputsize=full&apikey=ZNJ5L01YG7WWSI2T&datatype=csv']
APIs = pd.DataFrame({'url': URLs,
                     'datecol': ['Date','timestamp'],
                     'pricecol': ['Close','close'],
                     'isUnixdate': [True, False],
                     'cookies':[{'B':'apch901d6c7ag&b=3&s=91'},{}]},
                    index = ['Yahoo','Alphavantage'])

# information for dowloading exchange rates from Bank of Canada
fxrate_url = 'https://www.bankofcanada.ca/valet/observations/FXUSDCAD/csv?start_date=var_start&end_date=var_end'
fxrate_datecol = 'date'
fxrate_ratecol = 'FXUSDCAD'
fxrate_skip = 8

choice_all = 0
# =============================================================================
# Misc supporting functions
# =============================================================================
def symbolVariants(symbol):
    """ Return a list of variants for symbols with 2 dots (for class).
    Example: RCI.B.TO returns [RCI.B.TO, RCIB.TO, RCI-B.TO, RCI_B.TO] """

    alternatives = ['-','_','.','']
    if '.B' in symbol:
        return [symbol.replace('.',char,1) for char in alternatives]
    else:
        return [symbol]

def date2unix(date): return str(round(dt.datetime.strptime(date, "%Y-%m-%d").timestamp()))
def unix2date(timestamp): return dt.datetime.fromtimestamp(int(timestamp))

# =============================================================================
# Functions for downloading stock prices
# =============================================================================
def downloadPriceSingle(symbol,folder,start,end,source,varysymbol):
    """ Download daily price of a single stock, return 1 if succeed. """

    url = APIs.url[source].replace('var_start',start).replace('var_end',end)
    
    # try different variants of symbol (enabled by default)
    list2try = symbolVariants(symbol) if varysymbol else [symbol]
    for variant in list2try:
        print('Downloading daily price for %8s ... ' % variant,end='')
        
        # if an error occurs skip to the next symbol variant in the for loop
        try:
            url_variant =  url.replace('var_symbol',variant)
            page = requests.get(url_variant, cookies = APIs.cookies[source])
        except:
            print('Python requests error!')
            continue
        if page.status_code != 200:
            print('Server response error %d!' % page.status_code)
            continue
        # extrace top 100 characters of content to check for error message
        top = page.content[:100].lower().decode('utf-8')
        if ('error' in top) or ('invalid' in top): 
            print('Symbol seems invalid!')
            continue

        print('Completed.')
        
        # if no error occurs, process and save page content
        raw = pd.read_csv(io.StringIO(page.content.decode('utf-8')),
                          na_values='null')
        data = pd.DataFrame({'Date': raw[APIs['datecol'][source]],
                             'Price': raw[APIs['pricecol'][source]]})
        data = data[data['Price']!=0]
        data = data.dropna()
        
        global choice_all
        if os.path.exists(folder+'/'+symbol+'.csv'):
            if choice_all == 0:
                print('File '+symbol+'.csv already existed. What would you like to do?')
                print('1. Update the old data')
                print('2. Overwrite the old file')
                print('3. Skip and keep old file')
                print('4. Update for all')
                print('5. Overwrite for all')
                print('6. Skip for all')
                choice = input('Select by entering a number: ')
                while True:
                    try: 
                        choice = int(choice)
                        if (choice <= 6) and (choice >= 1): break
                        else: choice = input('Invalid input, please enter a number: ')
                    except: choice = input('Invalid input, please enter a number: ')
                if choice > 3: 
                    choice = (choice - 1) % 3 + 1 
                    choice_all = choice
            else: choice = choice_all
                
            if choice == 1:
                old = pd.read_csv(folder+'/'+symbol+'.csv')
                data = pd.concat([old,data])
            elif choice == 2: pass
            elif choice == 3: return 1
                
        data = data.drop_duplicates()    
        data.sort_values(by='Date', kind='quicksort', inplace=True)
        if not os.path.exists(folder): os.makedirs(folder)
        data.to_csv(folder+'/'+symbol+'.csv',index=False,float_format='%.2f')

        # save raw data as backup
        folder_raw = folder + '/raw/' + source
        date_min = raw[APIs['datecol'][source]].min()
        date_max = raw[APIs['datecol'][source]].max()
        filename = '_'.join([symbol,'from',date_min,'to',date_max])
        if not os.path.exists(folder_raw): os.makedirs(folder_raw)
        open(folder_raw + '/' + filename + '.csv','wb').write(page.content)
        
        return 1
    
    # next line runs only if all variants fail
    return 0

def downloadPrice(symbol, folder = price_folder,
                  start = '1970-01-02', end = today,
                  source = 'Yahoo', varysymbol=True, trials=3):
    """ 
    Download daily price for a single or a list of stocks.
    Return a list of symbols that fail to download.
    """
    global choice_all # user input choice for all existing data files
    choice_all = 0
    start = date2unix(start) if APIs.isUnixdate[source] else start
    end = date2unix(end) if APIs.isUnixdate[source] else end
    symbol = [symbol] if type(symbol) == str else symbol
    
    for i in range(trials): # try to download multiple times
        downloaded = [] # list of symbols already downloaded
        for each_symbol in symbol:
            if downloadPriceSingle(each_symbol,folder,start,end,source,varysymbol) == 1:
                downloaded.append(each_symbol)
        # remove downloaded symbols from the list
        for s in downloaded: symbol.remove(s)
        if len(symbol) == 0:
            print('\nSuccessfully downloaded prices for all symbols.')
            print('Relative path to downloaded data:', folder)
            break
        else:
            if i < trials-1:
                print('\nRe-attempting to download unsuccessful symbols.')
            else:
                print('\nAfter',trials,'attempts still failed to download:')
                for each_symbol in symbol: print(each_symbol)
                print('Relative path to downloaded data:', folder)
                return symbol

# =============================================================================
# Functions for loading stock prices from data files
# =============================================================================
def loadPriceSingle(symbol,folder,start,end):
    """ Load price of symbol into a series, indexed by dates. """
    
    try:
        data = pd.read_csv(folder+'/'+symbol+'.csv', na_values='null')
        data['Date'] = pd.to_datetime(data['Date'])
    except:
        print('Cannot find',symbol+'.csv','in the specified folder.')
        return []
        
    # if optional date inputs are not specified then use full range
    start = data['Date'].min() if start == '' else pd.to_datetime(start)
    end = data['Date'].max() if end =='' else pd.to_datetime(end)    
    
    data = data[(data['Date'] >= start) & (data['Date'] <= end)]
    # forward fill prices for weekends and holidays using merge_asof
    date_range = pd.DataFrame({'Date':pd.date_range(start,end)})    
    data = pd.merge_asof(date_range,data,on='Date').dropna()
    
    return pd.Series(data['Price'].tolist(),index=data['Date'].tolist())

def loadPrice(symbol, folder=price_folder, start='', end=''):
    """ 
    Load price of a symbol and return a series indexed by dates.
    If input is a list of symbols return a dictionary of series.
    """
    
    symbol = [symbol] if type(symbol) == str else symbol
    data = {}
    for each_symbol in symbol:
        prices = loadPriceSingle(each_symbol,folder,start,end)
        if len(prices) != 0:
            data[each_symbol] = prices
    return data

# =============================================================================
# Functions for downloading and loading exchange rates
# =============================================================================

def downloadFxRate(folder,start,end):
    """ Download exchange rates and update old data file """
    
    print('Downloading exchange rates from Bank of Canada ... ',end='')
    url = fxrate_url.replace('var_start',start).replace('var_end',end)
    try:
        page = requests.get(url)
    except:
        print('Python requests error!')
    if page.status_code != 200:
        print('Server response error %d!' % page.status_code)
        print('Failed to download exchange rates.')
        return 0
    print('Completed.')
    raw = pd.read_csv(io.StringIO(page.content.decode('utf-8')), 
                      skiprows = fxrate_skip, na_values = ' Bank holiday')
    data = pd.DataFrame({'Date':raw['date'], 'Rate':raw['FXUSDCAD']})
    data = data[data['Rate']!=0].dropna()
    #print(data)
    
    if os.path.exists(folder + '/' + fxrate_file):
        print('Updating existed rate data in %s ............ '%fxrate_file,end='')
        old = pd.read_csv(folder +'/' + fxrate_file)
        data = pd.concat([old,data])
    else:
        print('Saving rate data to the new file %s ......... '%fxrate_file,end='')

    data = data.drop_duplicates()    
    data.sort_values(by='Date', kind='quicksort', inplace=True)
    if not os.path.exists(folder): os.makedirs(folder)
    data.to_csv(folder+'/'+fxrate_file,index=False,float_format='%.4f')
    print('Completed.')
    # save raw data as backup
    folder_raw = folder + '/raw'
    date_min = raw[fxrate_datecol].min()
    date_max = raw[fxrate_datecol].max()
    filename = '_'.join(['from',date_min,'to',date_max])
    if not os.path.exists(folder_raw): os.makedirs(folder_raw)
    open(folder_raw + '/' + filename + '.csv','wb').write(page.content)

def loadFxRate(folder=fxrate_folder, start='2012-01-03', end=today):
    """  Load FX rate from file and return a series indexed by dates. """

    if os.path.exists(folder + '/' + fxrate_file):
        data = pd.read_csv(folder +'/' + fxrate_file)
        data['Date'] = pd.to_datetime(data['Date'])
        date_max = data['Date'].max()
        delta = pd.to_datetime(today) - date_max
        if delta.days >= 1:
            print('Latest exchange rate was %d days old.' % delta.days)
            downloadFxRate(folder, start, end)
    else:
        print('File %s cannot be found in the specified folder.' % fxrate_file)
        downloadFxRate(folder, start, end)
    
    data = pd.read_csv(folder +'/' + fxrate_file)
    data = data[(data['Date'] >= start) & (data['Date'] <= end)]
    data['Date'] = pd.to_datetime(data['Date'])
    date_range = pd.DataFrame({'Date':pd.date_range(start,end)})    
    data = pd.merge_asof(date_range,data,on='Date').dropna()

    return pd.Series(data['Rate'].tolist(),index=data['Date'].tolist())
    