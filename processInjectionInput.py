# -*- coding: utf-8 -*-
"""
Created on Sat Dec  1 09:21:40 2018

@author: @gary.allison

This code is used to take ODNR files for Brine disposal fee and 
eventually create a file to be used to show overall injection volumes.

The ODNR data have several limitations that we must find and account for: 
    - data type consistency, 
    - lumped API numbers
    - typos
    - different file formats across the years etc.
    

"""
import pandas as pd
import numpy as np
import pandas.api.types as ptypes
from validAPI import getAPI10

##### --------------------------------------------------
####           Input file definitions
##### --------------------------------------------------
# set data dirs for input files and for resulting output files
datadir = './sources/'
outdir = './out/'
indir = datadir+'OH_injection/'
pre_proc_out = outdir+'injection_tall_pre.csv'


# input files are in four different formats:
# for the oldest, specify filename, year and quarter:  tuple (filename,yr,q)
# all columns are named the same!!
fn_old = [('OH_1ST QUARTER 2011 BRINE DISPOSAL FEES.xls',2011,1),
          ('OH_2ND QUARTER 2011 BRINE DISPOSAL FEES.xls',2011,2),
          ('OH_3RD QUARTER 2011 BRINE DISPOSAL FEES-1.xls',2011,3),
          ('OH_4TH QUARTER 2010 BRINE DISPOSAL FEES.xls',2010,4),
          ('OH_4TH QUARTER 2011 BRINE DISPOSAL FEES.xls',2011,4),
          ('OH_Brine Disposal Fee - 3rd Quarter 2010-2.xls',2010,3)]

# the 2012 file is ina funky state - the set of worksheets have two different formats: a blend of old and main
# so we have to process it separately
fn_2012 = 'OH_BRINE DISPOSAL FEES FOR 2012.xls'

# bulk of the data are here - first four worksheets are quarters. 
# Total worksheet (the fifth one) is ignored
# specify the filename and the year: tuple: (filename,year)
fn_2013_17 = [('BRINE DISPOSAL FEES FOR 2013.xlsx',2013),
              ('BRINE DISPOSAL FEES FOR 2014.xlsx',2014),
              ('BRINE DISPOSAL FEES FOR 2015.xlsx',2015),
              ('BRINE DISPOSAL FEES FOR 2016.xlsx',2016), 
              ('BRINE DISPOSAL FEES FOR 2017.xlsx',2017)]

# Finally, the current file is of a different format and must also
# be treated separately.  It currently includes all quarters of the 
# year (even if they are in the future) and on a single worksheet
fn_2018_plus = [('BRINE DISPOSAL FEES FOR 2018.xlsx',2018),
              ('BRINE DISPOSAL FEES FOR 2019.xlsx',2019)]

# The text file with the records to collapse into one
aggfn = 'aggregateAPI.txt'

# We define these temporary file to examine output in progress
tempf = outdir+'temp.csv'
tempf1 = outdir+'temp1.csv'


def fetchAggregateList(fn=aggfn):
    agglist = []
    aggaction = {}
    with open(fn) as f:
        f.readline() # ignore header
        for ln in f.readlines():
            lst = ln.split('|')
            key = (lst[0],int(lst[1]),int(lst[2]))
            agglist.append(key)
            aggaction[key] = lst[3] # what to do when you find a match?
    return agglist, aggaction
agglist, aggaction = fetchAggregateList()
#print(agglist)

def is_on_AggregateList(API10,yr,q):
    if (API10,yr,q) in agglist:
        #print(f'Aggregating {API10}')
        return True
    return False

def getCollapseSet(ser,yr,q):
    # return list of APIs from the AggList to colllapse
    clst = []
    for index,row in ser.iteritems():
        if is_on_AggregateList(row,yr,q):
            clst.append(row)
    cset = set(clst)            
    return cset

##### --------------------------------------------------
#####           Input file readers
##### --------------------------------------------------
    
#### -------------------------2010 - 2011 ---------------
def read_old(fn,yr,quar,review=False,flag_problems=True):
    # read excel file and produce a pandas dataframe
    # we keep only 4 columns from the sheet, ignore the header,
    # and skip several rows at the top.
    # Unlike later files, these have only 1 volume column with a 
    # label column to specify if it is from in-district or out-of-district
    # We must combine the two (in and out) into a single record
    d = pd.read_excel(indir+fn,skiprows=5,header=None,usecols=[7,8,10,11],
                     names=['CompanyName','APIstr','Vol','In_Out'])
    # some volumes cells contain the work 'zero', 
    d.Vol = d.Vol.where(d.Vol.str.lower().str.strip()!='zero',0)
    d.Vol = pd.to_numeric(d.Vol)
    
    # make all of in-out into lowercase
    d.In_Out = d.In_Out.str.lower()
    
    # some In_Out cells have 'zero' in them: assign them to In
    d.In_Out = d.In_Out.where(d.In_Out.str.lower().str.strip()!='zero','in')

    assert ptypes.is_numeric_dtype(d.Vol)
    assert ptypes.is_string_dtype(d.CompanyName)
    assert ptypes.is_string_dtype(d.APIstr)
    api10 = []
    for index, row in d.iterrows():
        api10.append(getAPI10(row[1],yr,quar,flag_problems=flag_problems))
        if review:
            print(f'{api10[-1]}, {row[1]},{yr},{quar}')
    d['API10'] = api10

    ### ---------- handle multiple entries for a given API  ---------
    cset = getCollapseSet(d.API10,yr,quar)
    #print(cset)
    for capi in cset:
        tmp = d[d.API10 == capi]
        action = aggaction[(capi,yr,quar)]
        if action == 'sum':
            vol = tmp.groupby(['API10','In_Out'])['Vol'].sum()
        else:
            print(f'UNRECOGINIZED ACTION for {capi}')
           
        # make into df
        vol = pd.DataFrame(vol)
        # always take last of collapsed - assuming it is most recent
        other = tmp.groupby(['API10','In_Out'])['APIstr','CompanyName'].last()
        mg = pd.merge(vol,other,left_index=True,right_index=True,
                      validate='1:1')
        mg.reset_index(level=[0,1],inplace=True)        
        tmp = d[d.API10 != capi] # drop the old
        d = pd.concat([tmp,mg],sort=True) # add the new
    ### --------------------------------------------------------------
    
    ### --------------- Make a meta df ----------------------------
    meta = d.copy().filter(['API10','APIstr','CompanyName'])
    meta = meta.groupby(['API10'],as_index=False)['APIstr','CompanyName'].first()    

    ### ----------------- snag all in-district records
    dIn = d[d.In_Out.str.lower().str[0]=='i'] #'In district'
    dIn = dIn.filter(['API10','Vol'])
    dIn.columns = ['API10','Vol_InDist']
# =============================================================================
#     print(f'{len(dIn)}, {len(dIn.API10.unique())}')
#     print(dIn[dIn.API10.duplicated()==True])
#     dIn.sort_values(by='API10').to_csv(tempf)
# =============================================================================
    assert len(dIn)==len(dIn.API10.unique())
    # put together with all
    meta = pd.merge(meta,dIn,how='left',on='API10',validate='1:1')
    meta.to_csv(tempf)
    
    
    ### --------------- snag all out-of-district records
    dOut = d[d.In_Out.str.lower().str[0]=='o']
    dOut = dOut.filter(['API10','Vol'])
    dOut.columns = ['API10','Vol_OutDist']
# =============================================================================
#     print(f'{len(dOut)}, {len(dOut.API10.unique())}')
#     print(dOut[dOut.API10.duplicated()==True])
#     dOut.sort_values(by='API10').to_csv(tempf)
# =============================================================================
    assert len(dOut)==len(dOut.API10.unique())
    meta = pd.merge(meta,dOut,how='left',on='API10',validate='1:1')
    meta.to_csv(tempf)
    
    meta['Year'] = yr
    meta['Quarter']= quar
    assert len(d.API10.unique())==len(meta)    
    return meta


######  -------------  Read the 2012 file --------------------
def read_2012(fn,review=False,flag_problems=True):
    # read excel file and produce a pandas dataframe
    # we keep only 4 columns from the sheet, ignore the header,
    # and skip several rows at the top.
    # This file has TWO different formats, so we must accomodate that (uc1 and uc2)    
    dlst = []
    uc1 = [1,2,4,8]   # sheet 0,2,3 are different from sheet 1
    uc2 = [7,8,10,14] # os we take from different columns
    for ws in [0,1,2,3]: # ws 1 is like 'main'; others like 'old'
        yr = 2012
        quar = ws+1 # worksheets are labeled 0-3
        print(f'Processing {yr} Q{quar}')
        if ws == 1:
            uc = uc1
        else:
            uc= uc2
        d = pd.read_excel(indir+fn_2012,skiprows=6,sheet_name=ws,
                         usecols=uc,header=None,
                         names=['CompanyName','APIstr','Vol_InDist','Vol_OutDist'])
        d.Vol_InDist = pd.to_numeric(d.Vol_InDist,errors='coerce')
        d.Vol_OutDist = pd.to_numeric(d.Vol_OutDist,errors='coerce')
        d = d.dropna(axis=0,subset=['CompanyName']) # no CompanyName = no record
        assert ptypes.is_numeric_dtype(d.Vol_InDist)
        assert ptypes.is_numeric_dtype(d.Vol_OutDist)
        assert ptypes.is_string_dtype(d.CompanyName)
        assert ptypes.is_string_dtype(d.APIstr)
        api10 = []
        for index, row in d.iterrows():
            api10.append(getAPI10(row[1],yr,quar,flag_problems=flag_problems))
            if review:
                print(f'{api10[-1]}, {row[1]},{yr},{quar}')
        d['API10'] = api10

        ### ---------- handle multiple entries for a given API  ---------
        cset = getCollapseSet(d.API10,yr,quar)
        print(f'  collapse set for this worksheet: {cset}')
        for capi in cset:
            tmp = d[d.API10 == capi]
            action = aggaction[(capi,yr,quar)]
            if action == 'sum':
                vol = tmp.groupby(['API10'])['Vol_InDist','Vol_OutDist'].sum()
            else:
                print(f'UNRECOGINIZED ACTION for {capi}')
            # make into df
            vol = pd.DataFrame(vol)
            # always take last of collapsed - assuming it is most recent
            other = tmp.groupby(['API10'])['APIstr','CompanyName'].last()
            mg = pd.merge(vol,other,left_index=True,right_index=True,
                          validate='1:1')
            mg.reset_index(level=[0],inplace=True)        
            tmp = d[d.API10 != capi] # drop the old
            d = pd.concat([tmp,mg],sort=True) # add the new
        ### --------------------------------------------------------------
# =============================================================================
#         print(f'{len(d)}, {len(d.API10.unique())}')
#         print(d[d.API10.duplicated()==True])
#         d.sort_values(by='API10').to_csv(tempf)
# =============================================================================
        assert len(d)==len(d.API10.unique())
        d['Year'] = 2012
        d['Quarter'] = quar 

        dlst.append(d)
        
    trans2012 = pd.concat(dlst,sort=True)
    #trans2012.to_csv(tempf)
    
    return trans2012

#### ---------------------- Main data files 2013-2017+ -------------

def read_2013_17(fn,yr,review=False,flag_problems=True):
    # read excel file and produce a pandas dataframe
    # we keep only 4 columns from the sheet, ignore the header,
    # and skip several rows at the top.

    dlst = []
    for ws in [0,1,2,3]: # four quarterly worksheets
        quar = ws+1
        print(f'Processing {yr} Q{quar}')
        d = pd.read_excel(indir+fn,skiprows=6,sheet_name=ws,
                         usecols=[0,1,2,4,8],header=None,
                         names=['AltName','CompanyName','APIstr','Vol_InDist','Vol_OutDist'])
        
        # have to re-order columns to match earlier formats
        d = d.filter(['CompanyName','APIstr','Vol_InDist','Vol_OutDist','AltName'])
        d.Vol_InDist = pd.to_numeric(d.Vol_InDist,errors='coerce')
        d.Vol_OutDist = pd.to_numeric(d.Vol_OutDist,errors='coerce')
        d.APIstr = d.APIstr.astype('str')
        d = d.dropna(axis=0,subset=['CompanyName']) # no CompanyName = no record
        

#        d['Year'] = yr
#        d['Quarter'] = quar
        assert ptypes.is_numeric_dtype(d.Vol_InDist)
        assert ptypes.is_numeric_dtype(d.Vol_OutDist)
        assert ptypes.is_string_dtype(d.CompanyName)
        assert ptypes.is_string_dtype(d.APIstr)
        api10 = []
        for index, row in d.iterrows():
            api10.append(getAPI10(row[1],yr,quar,flag_problems=flag_problems))
            if review:
                print(f'{api10[-1]}, {row[1]},{yr},{quar}')
        d['API10'] = api10

        ### ---------- handle multiple entries for a given API  ---------
        cset = getCollapseSet(d.API10,yr,quar)
        print(f'  collapse set for this worksheet: {cset}')
        for capi in cset:
            tmp = d[d.API10 == capi]
            action = aggaction[(capi,yr,quar)]
            if action == 'sum':
                vol = tmp.groupby(['API10'])['Vol_InDist','Vol_OutDist'].sum()
            elif action== 'last':
                vol = tmp.groupby(['API10'])['Vol_InDist','Vol_OutDist'].last()
            else:
                print(f'UNRECOGINIZED ACTION for {capi}')
            # make into df
            vol = pd.DataFrame(vol)
            # always take last of collapsed - assuming it is most recent
            other = tmp.groupby(['API10'])['APIstr','CompanyName'].last()
            mg = pd.merge(vol,other,left_index=True,right_index=True,
                          validate='1:1')
            mg.reset_index(level=[0],inplace=True)        
            tmp = d[d.API10 != capi] # drop the old
            d = pd.concat([tmp,mg],sort=True) # add the new
        ### --------------------------------------------------------------
# =============================================================================
#         print(f'{len(d)}, {len(d.API10.unique())}')
#         print(d[d.API10.duplicated()==True])
#         d.sort_values(by='API10').to_csv(tempf)
# =============================================================================
        assert len(d)==len(d.API10.unique())
        d['Year'] = yr
        d['Quarter'] = quar


        dlst.append(d)
    
    main = pd.concat(dlst,sort=True)
    main.to_csv(tempf)
    return main

#### ---------------------- Current data files (including future within year)
def read_2018_plus(fn,yr,review=False,flag_problems=True):
    # read excel file and produce a pandas dataframe
    # we keep only 4 columns from the sheet, ignore the header,
    # and skip several rows at the top.

    print(f'\nProcessing {fn},{yr}')
    d = pd.read_excel(indir+fn,skiprows=6,sheet_name=0,
                     usecols=[0,1,2,3,5,9],header=None,
                     names=['QtrStr','AltName','CompanyName','APIstr','Vol_InDist','Vol_OutDist'])
    d = d.dropna(axis=0,subset=['CompanyName'])
    d['Year'] = yr
    d['Quarter'] = d.QtrStr.str[0]
    d = d[d.Quarter != 'Y'] # drop the year total rows
    d.Quarter = pd.to_numeric(d.Quarter,errors='coerce')
    d.Vol_InDist = pd.to_numeric(d.Vol_InDist,errors='coerce')
    d.Vol_OutDist = pd.to_numeric(d.Vol_OutDist,errors='coerce')
    d.APIstr = d.APIstr.astype('str')
    d = d.filter(['CompanyName','APIstr','Vol_InDist','Vol_OutDist','AltName','Year','Quarter'])

    api10 = []
    for index, row in d.iterrows():
        quar = int(row[6])
        api10.append(getAPI10(row[1],yr,quar,flag_problems=flag_problems))
        if review:
            print(f'{api10[-1]}, {row[1]},{yr},{quar}')
    d['API10'] = api10

    #Because all quarters are on one sheet, but we have to verify that there
    # are no duplicates within a quarter, we apply the cset tests to
    # quarter subsets then concat at the end
    dlst = []
    for quar in [1,2,3,4]:
        print(f'  working on quarter {quar}')
        dq = d[d.Quarter==quar]
        ### ---------- handle multiple entries for a given API  ---------
        cset = getCollapseSet(dq.API10,yr,quar)
        print(f'  collapse set for this worksheet: {cset}')
        for capi in cset:
            tmp = dq[dq.API10 == capi]
            action = aggaction[(capi,yr,quar)]
            if action == 'sum':
                vol = tmp.groupby(['API10'])['Vol_InDist','Vol_OutDist'].sum()
            elif action== 'last':
                vol = tmp.groupby(['API10'])['Vol_InDist','Vol_OutDist'].last()
            else:
                print(f'UNRECOGINIZED ACTION for {capi}')
            # make into df
            vol = pd.DataFrame(vol)
            # always take last of collapsed - assuming it is most recent
            other = tmp.groupby(['API10'])['APIstr','CompanyName','Year','Quarter'].last()
            mg = pd.merge(vol,other,left_index=True,right_index=True,
                          validate='1:1')
            mg.reset_index(level=[0],inplace=True)        
            tmp = dq[dq.API10 != capi] # drop the old
            dq = pd.concat([tmp,mg],sort=True) # add the new

# =============================================================================
#         print(f'{len(dq)}, {len(dq.API10.unique())}')
#         print(dq[dq.API10.duplicated()==True])
#         dq.sort_values(by='API10').to_csv(tempf)
# =============================================================================
        assert len(dq)==len(dq.API10.unique()), f'{len(dq)} != {len(dq.API10.unique())}'
        dlst.append(dq)
        ### --------------------------------------------------------------
    d_curr = pd.concat(dlst,sort=True)


    return d_curr


def processAllFiles(review=False):
    dlst = []
    for fnl in fn_old:
        print(f'Processing {fnl[0]}')
        out = read_old(fnl[0],fnl[1],fnl[2],review=review)
        dlst.append(out)
    out = read_2012(fn_2012,review=review)
    dlst.append(out)
    for fnl in fn_2013_17:
        print(f'\n  ***** Processing {fnl[0]}')
        out = read_2013_17(fnl[0],fnl[1],review=review)
        dlst.append(out)
    for fnl in fn_2018_plus:
        print(f'\n  ***** Processing {fnl[0]}')
        out = read_2018_plus(fnl[0],fnl[1],review=review)
        dlst.append(out)
    final = pd.concat(dlst,sort=True)
    
    final['YrQ'] = final.Year.astype('str') + 'Q' + final.Quarter.astype('str')
    final.sort_values(by=['API10','Year','Quarter']).to_csv(pre_proc_out)

    return final.sort_values(by=['API10','Year','Quarter'])

if  __name__ == '__main__':
    out = processAllFiles()