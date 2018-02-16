# -*- coding: utf-8 -*-
# Author: Hoan Bui Dang
# Python: 2.7

""" 
Merge csv data from different bank accounts together in a standard format.
Follow the instruction for reading csv files for each account in accounts.csv.
"""

import numpy as np
import pandas as pd
import re # for regex
import glob # for file matching

folder = 'data'
info = pd.read_csv(folder + '/accounts.csv', skiprows=15)
info = info.fillna(0) # fill in 0 for blank values
int_cols = range(6,16) # columns to be converted to type int
info[info.columns[int_cols]] = info[int_cols].astype(int)
info.index = info.Account # use account names to label rows

all_cols=['Date','Transaction','Amount','BalanceEOD',\
            'Currency','Account','Type','Source']
data = pd.DataFrame()

def error(message, account='', errtype='Error'):
    print '\n' + '=' * 80
    if account == '': print errtype + ': ' + message
    else: print errtype + ' (account ' + account + '): ' + message
    print '=' * 80
    return
def tocents(collection_of_lists):
    """ return a list of sets of (integer) values in cents """
    return [{int(round(e*100)) for e in l} for l in collection_of_lists]
def flatten(list_of_lists):
    """ flatten a list of lists into a single list """
    return [e for x in list_of_lists for e in x]

data_list = []
verified_accounts = []
nobalance_accounts = []
ignored_accounts = []

#for a in ['Questrade_CAD']: # for testing a specific account
for a in info.Account: # loop through all account names
    print 'Processing data for account ' + a + ' '*20

    #==========================================================================
    # set default values and check information in info
    #==========================================================================
    
    account_type = info.Type[a] if info.Type[a] != 0 else ''
    if account_type == '': error('Please specify account type.', a, 'Warning')
    path = folder+'/'+info.Path[a] if info.Path[a] != 0 else folder+'/'+a
    currency = info.Currency[a] if info.Currency[a] != 0 else 'CAD'
    amount_col = info.Amount[a] - 1
    out_col = info.Out[a] - 1
    in_col = info.In[a] - 1
    amt_sign = 1 if info.AmtSign[a] == 0 else info.AmtSign[a]
    out_sign = 1 if info.OutSign[a] == 0 else info.OutSign[a]
    in_sign = 1 if info.InSign[a] == 0 else info.InSign[a]
    balance_col = info.Balance[a] - 1
    date_col = info.Date[a] - 1
    if date_col == -1:
        error('No date column provided, account will be ignored.', a)
        ignored_accounts.append(a)
        continue
    trans_col = info.Transaction[a]
    if trans_col == 0:
        error('No transaction column, account will be ignored', a)
        ignored_accounts.append(a)
        continue
    anchor_bal = info.BalanceEOD[a]
    anchor_date = info.On[a]
    if anchor_date != 0: 
        try: # convert anchor_date to datetime type
            anchor_date = pd.to_datetime(str(anchor_date))
            date_part = pd.to_datetime(anchor_date.date())
            if date_part != anchor_date:
                error('Invalid date for anchor balance.',a,'Warning')
                anchor_date = 0
        except:
            error('Invalid date for anchor balance.',a,'Warning')
            anchor_date = 0
    
    #==========================================================================
    # import, convert, and combine data from files --> raw
    #==========================================================================
    
    all_files = glob.glob(path + '/*.csv') 
    if len(all_files) == 0:
        error('No csv file found, please check data path.', a)
        ignored_accounts.append(a)
        continue # skip to the next account

    skip_account = False
    df_list = [] # list of dataframes containing imported data
    for f in all_files:
        data_f = pd.read_csv(f, header=None, skiprows=info.Skip[a])
        raw_ncols = len(data_f.columns) # number of columns in raw data
        fn = f[len(path)+1:] # filenames only (no path)
        data_f['Source'] = fn # add a column to track data source
        
        # ==============  convert numeric columns to float type  ==============
        for col in ['Amount','In','Out','Balance']:
            c = info[col][a] - 1
            if c != -1:
                if data_f[c].dtype != np.float64:
                    try:
                        # remove '$' and ',' in string then convert to float
                        data_f[c].replace('[\$,)]','',regex=True, inplace=True)
                        # replace '(' with minus sign
                        data_f[c].replace('[(]','-',regex=True, inplace=True)
                        data_f[c] = data_f[c].astype(float)
                    except:
                        error(col+' in '+fn+' is not numeric.',a)
                        ignored_accounts.append(a)
                        skip_account = True
                    
        # ==========  convert date column to panda.datetime type  =============
        if any(data_f[date_col].isnull()):
            error('Dates missing in file '+fn, a)
            print 'Please check data at the following row(s):'
            print data_f[data_f[date_col].isnull()]
            ignored_accounts.append(a)
            skip_account = True
            break
        try:
            data_f['Date'] = pd.to_datetime(data_f[date_col].astype(str))
            # check to make sure not converted to time
            date_part = pd.to_datetime(data_f['Date'].dt.date)
            bad_rows = (date_part != data_f['Date'])
            if any(bad_rows):
                error('Date converted incorrectly in file '+fn+'.', a)
                print 'Incorrect dates at the following entries:'
                print data_f[range(0,min(5,raw_ncols))][bad_rows]
                ignored_accounts.append(a)
                skip_account = True
                break
        except:
            error('Invalid date format, account will be ignored.', a)
            ignored_accounts.append(a)
            skip_account = True
            break
        
        # =============  reverse order if dates are decreasing  ===============
        if data_f['Date'][0] >= data_f['Date'].iloc[-1]:
            data_f = data_f.iloc[::-1]
            
        df_list.append(data_f)
        # ===============  end of loop through source files  ==================
        
    if skip_account: continue
    raw = pd.concat(df_list, ignore_index=True)

    #==========================================================================    
    # combine transaction columns & basic string cleaning
    #==========================================================================
    #
    #    If trans_col has more than 1 digits, it's interpreted that
    #    there are multiple transaction description columns, each
    #    specified by a digit of trans_col. Data in these columns will
    #    be combined in the same order as that of the digits.
    # 
    # String cleaning:
    # -  Collapse multiple white spaces
    # -  # Remove non-alphanumeric characters
    # -  Remove trailing and heading white spaces
    
    trans_list = [int(x)-1 for x in str(trans_col)]
    raw[trans_list] = raw[trans_list].fillna('')
    raw['Transaction'] = raw[trans_list].apply(lambda x:' '.join(x),1)
    raw['Transaction'].replace('\s+',' ',regex=True,inplace=True)
    # raw['Transaction'].replace('\W+',' ',regex=True,inplace=True)
    raw['Transaction'] = raw['Transaction'].str.strip()
    bad_rows = (raw['Transaction']=='')
    if any(bad_rows):
        error('Transaction description missing.', a, 'Warning')
        print 'Missing transaction description at the following entries:'
        print raw[['Source']+range(0,min(5,raw_ncols))][bad_rows]
    
    #==========================================================================
    # calculate Amount if given In and Out
    #==========================================================================
    
    if amount_col == -1: 
        if (in_col == -1) and (out_col == -1): 
            error('Must either specify Amount column,' + \
                  ' or both In & Out columns.', a)
            ignored_accounts.append(a)
            continue
        else:
            bad_rows = raw[in_col].isnull() & raw[out_col].isnull()
            if any(bad_rows):
                error('Amount missing, account will be ignored.',a)
                print 'Missing amounts at the following entries:'
                print raw[['Source']+range(0,min(5,raw_ncols))][bad_rows]
                ignored_accounts.append(a)
                continue
            raw['Amount']  = in_sign*raw[in_col].fillna(0)
            raw['Amount'] -= out_sign*raw[out_col].fillna(0)
    else:
        bad_rows = raw[amount_col].isnull()
        if any(bad_rows):
            error('Amount missing, account will be ignored.',a)
            print 'Missing amounts at the following entries:'
            print raw[['Source']+range(0,min(5,raw_ncols))][bad_rows]
            ignored_accounts.append(a)
            continue
        raw['Amount'] = amt_sign*raw[amount_col]
        if (in_col > -1) or (out_col > -1):
            error('In & Out ignored in presence of Amount',a,'Warning')
    
    #==========================================================================
    # copy basic data to new dataframe --> basic
    #==========================================================================
#    basic = pd.DataFrame(columns=basic_cols)
    basic = pd.DataFrame()
    copy_cols = ['Date','Transaction','Amount','Source']
    basic[copy_cols] = raw[copy_cols]
    basic['Currency'] = currency
    basic['Account'] = a
    basic['Type'] = account_type
    basic['Balance'] = raw[balance_col] if balance_col >= 0 else 0
    
    #==========================================================================
    # identify and remove duplicates --> nodup
    #==========================================================================
    #
    # General rules:
    #    Duplicates from the same source files are allowed, as it might
    #    happen that there are multiple identical transactions. Dups from 
    #    different sources, however, indicate data redundancy (for example
    #    due to downloading the same data multiple times). If a transaction
    #    is found in multiple sources, it will be kept from the source with
    #    max occurences, and its occurences in that source will also be kept.
    #
    # b1:
    #    Group by all columns and count duplicates from each value + source.
    # kept_rows:
    #    Group by value only, then return indices where count is max (among
    #    different sources) for each value.
    # b2:
    #    Not kept rows (to be removed from original data).
    # nodup:
    #    Left merge b2 to original data on all columns except Count.
    #    The result will have Nan Count on rows that weren't merged, which
    #    are rows to be kept.
    
    b1 = basic.groupby(list(basic),sort=False).size().reset_index(name='Count')
    value_cols = list(basic.columns.drop('Source'))
    kept_rows = b1.groupby(value_cols,sort=False)['Count'].idxmax()
    b2 = b1.loc[b1.index.drop(kept_rows)]
    nodup = pd.merge(basic,b2,how='left',on=list(basic))
    nodup = nodup[nodup['Count'].isnull()].drop('Count',1)
    
    if len(b2) > 0 :
        error('Duplicated data found.', a, 'Warning')
        print 'The following duplicated entries were removed: \n'
        print b2[['Date','Transaction','Amount','Source']]

    #==========================================================================
    # verify data if Balance column is provided
    #==========================================================================    
    #
    # gd: 
    #    For each date: sum up all transaction amounts and combine all
    #    imported balances to a list (gd.Balance contains lists of balances).
    # d1:
    #    Cumulatively sum Amount to calculate day-end balance
    #    then subtract it from each value in gd.Balance and
    #    convert to a list of sets, in unit of cents (integer values).
    #    If imported balances match calculated day-end balances
    #    then every set in d1 should contain a common value 0, 
    #    or a common constant offset equal to the initial balance.
    # d2:
    #    For the case imported Balance has wrong sign (e.g. credit cards).
    #    To determine the sign of Balance (i.e. whether to use d1 or d2)
    #    count the occurences of the most frequent value in d1 and d2
    #    and pick the one with the higher count.
    # tocents():
    #    Multiply by 100 and convert to integer values (in cents) to
    #    allow accurate results when taking intersection or using
    #    np.unique (float numbers can't be compared exactly).
    #    Output is a list of sets.
    # flatten():
    #    Flatten a collection of lists into a single list.
    #
    # Other notes:
    # 1. The star * is to unlist the argument for set.intersect().
    # 2. Find most frequent element in list 'x = max(list,key=list.count)'
    #    then find its frequency by 'list.count(x)',
    #    or by using 'values, counts = np.unique(list, returns_counts=True)'
    #    and take 'max(counts)'.
   
    if balance_col >= 0:
        gd = nodup.groupby('Date').agg({'Amount':'sum','Balance':'unique'})
        d1 = tocents( gd['Balance'] - gd['Amount'].cumsum())
        d2 = tocents(-gd['Balance'] - gd['Amount'].cumsum())
        count1 = max(np.unique(flatten(d1),return_counts=True)[1])
        count2 = max(np.unique(flatten(d2),return_counts=True)[1])
        (d0,balance_sign) = (d1,1) if count1 >= count2 else (d2,-1)
        
        if len(set.intersection(*d0)) == 1:
            verified_accounts.append(a)
            balance_diff = float(set.intersection(*d0).pop())/100
        else:
            error('Data inconsistent with balance.',a,'Warning')
            print 'There seem to be data gaps between:\n'
            # find consecutive sets in d0 that do not intersect
            # these disconnections among sets in d0 indicate data gaps
            intersection = d0[0]
            i = 0
            for s in d0[1:]: # loop through all sets s in d0, starting from 1
                intersection.intersection_update(s) # take intersection with s
                if len(intersection) == 0:
                    print ' ',gd.index[i].date(),' and ',gd.index[i+1].date()
                    intersection = s
                i += 1

    #==========================================================================
    # calculate end-of-day balance (EOD)
    #==========================================================================
    #
    # If balance data is available:
    #    If data is consistent: group amounts by date, cumsum to calculate EOD.
    #    If data is inconsistent: take EOD to be the last balance of each date.
    #    Ignore anchor balance.
    # If balance data is not available:
    #    Calculate EOD assuming zero initial balance.
    #    If anchor balance is provided, use it to offset EOD so that EOD of
    #    the nearest date on or before the anchor date is equal to the
    #    anchor balance. If anchor date is earlier than all data dates, then
    #    use anchor balance as the initial balance.

    nodup.sort_values(by='Date', kind='mergesort', inplace=True)
    nodup.reset_index(drop=True, inplace=True)
    g = nodup.groupby('Date', sort=False)
    if (balance_col >= 0) & (a not in verified_accounts):
        nodup = nodup.join(balance_sign*g.Balance.last(),on='Date',rsuffix='EOD')
    else:
        eod = g.Amount.sum().cumsum()
        if (balance_col >= 0) & (a in verified_accounts):
            eod += balance_diff
            if anchor_date != 0: 
                error('Anchor balance ignored in presence of balance data.',a,'Warning')
        elif anchor_date != 0:
            if anchor_date < min(eod.index):
                eod += anchor_bal
                error('Anchor balance date out of data range.',a,'Warning')
                print 'If possible, use an anchor date between: ',\
                      min(eod.index).date(),' & ',max(eod.index).date(),'.'
                print 'Anchor balance was considered as initial balance.'
            else:
                idx = eod[eod.index<=anchor_date].reset_index().Date.idxmax()
                eod += anchor_bal - eod.iloc[idx]
                if anchor_date > max(eod.index):
                    error('Anchor balance date out of data range.',a,'Warning')
                    print 'If possible, use an anchor date between: ',\
                      min(eod.index).date(),' & ',max(eod.index).date(),'.'
                    print 'Anchor balance was considered as balance of the last date.'
        else:
            #error('No balance data or anchor balance provided.',a,'Warning')
            nobalance_accounts.append(a)
        nodup = nodup.join(eod.to_frame('BalanceEOD'),on='Date')
        
        # add row to extend balance date for active accounts
        if info.Active[a] == 1:
            newline = nodup.loc[len(nodup)-1]
            newline.Transaction = 'Automatic balance update'
            newline.Source = 'merge.py'
            newline.Amount = 0
            nodup.loc[len(nodup)] = newline
            #nodup = nodup.append(newline)

    data_list.append(nodup)
    # ================= end of for loop through all accounts ==================
    
# concatenate data from all accounts and export to csv
if len(data_list) > 0: 
    data = pd.concat(data_list, ignore_index=True)[all_cols]

    date_max = data.Date.max() + pd.DateOffset(1)
    data.loc[data.Source=='merge.py','Date'] = date_max
    
    
    # ========================= USD rate conversion ===========================
    # Notes:
    # 1. For a stable sorting algorithm (i.e. preserving the original order
    #    among elements of equal values) set kind='mergesort'.
    # 2. Merge doesn't preserve index, but one can still do so using this
    #    trik: df.reset_index().merge(...).set_index('index')
    # 3. To merge by nearest key use merge_asof (pandas version 0.19+). Key
    #    must be sorted before using merge_asof.
    print ''
    execfile('getrate.py') # get dataframe 'rate' with columns ['Date','USD']
    data.sort_values('Date',kind='mergesort',inplace=True)
    data = pd.merge_asof(data.reset_index(),rate,on='Date').set_index('index')
    data.sort_index(inplace=True)
    if rate.Date.min() > data.Date.min() or rate.Date.max() < data.Date.max():
        error('Please update rate data.','','Warning')
    
    for col in ['Amount','BalanceEOD']:
        USD_rows = (data[col]*data['USD'])[data.Currency=='USD']
        CAD_rows = data[col][data.Currency=='CAD']
        data[col+'_CAD'] = pd.concat([CAD_rows,USD_rows])
        
    
    data.to_csv(folder+'_merged.csv',index=0,date_format='%Y-%m-%d',float_format='%.2f')
    print '\nData have been successfully merged and saved as "'+folder+'_merged.csv".'
    
    if len(ignored_accounts) > 0:
        print '\nAccounts that were ignored due to errors:'
        for a in np.unique(ignored_accounts): print ' ', a    
    if len(verified_accounts) > 0:
        print '\nAccounts with data verified to be consistent with balance:'
        for a in verified_accounts: print ' ', a
    if len(nobalance_accounts) > 0:
        print '\nPlease provide anchor balance for the following accounts:'
        for a in nobalance_accounts: print ' ', a
else: print '\nNo valid data to merge.'

