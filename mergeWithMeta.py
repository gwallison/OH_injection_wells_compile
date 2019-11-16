# -*- coding: utf-8 -*-
"""
Created on Mon Dec  3 11:30:03 2018

@author: gary.allison

These routines take the pre-processed injection results and try to match
API numbers
"""

import pandas as pd
import numpy as np
import pandas.api.types as ptypes
from processInjectionInput import processAllFiles

##### --------------------------------------------------
####           Input file definitions
##### --------------------------------------------------
# set data dirs for input files and for resulting output files
datadir = './sources/'
outdir = './out/'
indir = datadir+'OH_injection/'

### metadata sources ###
SWDfn = indir+'Copy of SWD locations - July_2018.xls'
ODNR_permit_pickle = outdir+'ODNR_permit.pkl'
ODNR_injection_pickle = outdir+'ODNR_injection.pkl'

xlatefn = 'xlateAPI.txt'
xlate_excel = 'xlateAPI.xls'
pre_proc_out = outdir+'injection_tall_pre.csv'
inj_meta = outdir+'injection_meta_list.csv'
tempf = outdir+'temp.csv'

### --------------------------------------------------------------
def getTallSet(fn=pre_proc_out):
    return pd.read_csv(fn)

ppout = pd.read_csv(pre_proc_out)

def prepInjData(fn=pre_proc_out):
    ppout = pd.read_csv(pre_proc_out)
    ppAPI = ppout.groupby(['API10'],as_index=False)['CompanyName'].last()
    assert len(ppAPI) == len(ppAPI.API10.unique())
    return ppAPI

def prepSWD(fn=SWDfn):
    SWD_df = pd.read_excel(fn)
    SWD_df['API10'] = SWD_df['API #'].astype('str').str[0:10]
    SWD_df.API10 = SWD_df.API10.astype('str')
    # note that lat/lon are reversed in the file, so I fix in the columns
    SWD_df.columns = ['API','Owner','WellName','County','Township','Longitude','Latitude','WellStatus','API10']
    assert len(SWD_df) == len(SWD_df.API10.unique())
    return SWD_df

def prepODNRinjList(fn=ODNR_injection_pickle):
    ODNRi = pd.read_pickle(fn)
    #print(f'ODNRi col: {ODNRi.columns}')
    assert len(ODNRi) == len(ODNRi.API10.unique())
    return ODNRi

def prepODNRPermitList(fn=ODNR_permit_pickle):
    ODNRp = pd.read_pickle(fn)
    #print(ODNRp.columns)
    cols = ['API','County','Owner', 'Township', 'PermitDate', 'WellName',
       'WellNumber', 'Latitude', 'Longitude', 'API10']
    ODNRp.columns = cols
    ODNRp = ODNRp.groupby('API10',as_index=False)['API','County','Owner', 
                         'Township', 'PermitDate', 'WellName',
                         'WellNumber', 'Latitude', 'Longitude'].last()
    #print(f'Len of ODNRp = {len(ODNRp)}')
    #assert len(ODNRp) == len(ODNRp.API10.unique())
    return ODNRp

def joinMetaWithInjAPI(metadf,injdf,sourcename):
    mg = pd.merge(metadf,injdf,how='outer',on='API10',indicator=True,validate='1:1')
    mg.sort_values(by='API10').to_csv(tempf)
    match = mg[mg._merge=='both'].copy()
    match = match.drop('_merge',axis=1)
    leftover = mg[mg._merge=='right_only'].copy()
    leftover = leftover.filter(['API10','CompanyName'],axis=1)
    match['meta_source'] = sourcename
    print(f'input={len(injdf)}. For {sourcename}: matched={len(match)}, unmatched={len(leftover)}')    
    return match,leftover

def makeWholeSet():
    ppAPI = prepInjData()
    swd = prepSWD()
    ODNRi = prepODNRinjList()
    ODNRp = prepODNRPermitList()
    match1,leftover1 = joinMetaWithInjAPI(swd,ppAPI,'SWD-july2018')
    match2,leftover2 = joinMetaWithInjAPI(ODNRi,leftover1,'ODNR Injection scrape')
    match3,leftover3 = joinMetaWithInjAPI(ODNRp,leftover2,'ODNR Permit scrape')
    leftover3['meta_source'] = 'No match'
    
    wholeset = pd.concat([match1,match2,match3,leftover3],sort=True)
    wholeset.sort_values(by='API10').to_csv(inj_meta)
    return wholeset

def addLatLonToTall():
    whole = makeWholeSet()
    whole = whole.filter(['API10','Latitude','Longitude'],axis=1)
    whole.Latitude = whole.Latitude.fillna(40)
    whole.Latitude = whole.Latitude.fillna(-83)
    ppout = getTallSet()
    mg = pd.merge(whole,ppout,on='API10',how='right',validate='1:m')
    return mg

def getNoMatchSet():
    whole = makeWholeSet()
    return whole[whole.meta_source=='No match'].copy()
    
def makeXlateSpreadsheet(fn=xlatefn):
    xlate = pd.read_csv(fn,sep='|')
    print(len(xlate))
    xlate.to_excel(xlate_excel)
    
def getNoMatchRecLen():   
    nm = getNoMatchSet()
    ppout = pd.read_csv(pre_proc_out)
    voli = []
    volo = []
    apis = []
    wlst = []
    print(f'Len ppout = {len(ppout)}')
    for api10 in nm.API10.iteritems():
        test = ppout[ppout.API10== api10[1]]
        voli.append(test.Vol_InDist.sum())
        volo.append(test.Vol_OutDist.sum())
        apis.append(api10[1])
        lst = []
        for row in test.YrQ.iteritems():
            lst.append(row[1])
        #print(f'For {api10[1]}, dates = {lst}')
        #print(f'For {api10[1]}, num rec= {len(test)}, voli = {voli}, volo = {volo}')
        wlst.append(lst)
    summary = pd.DataFrame({'API10':apis,'Vol_InDist':voli,'Vol_OutDist':volo,
                            'Dates':wlst})
    return summary

if __name__ == '__main__':
    tall = addLatLonToTall()
    tall.to_csv(tempf)