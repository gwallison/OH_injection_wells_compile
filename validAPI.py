# -*- coding: utf-8 -*-
"""
Created on Sat Dec  1 10:17:57 2018

@author: gary.allison

This code is used to translate an APINumber string from the ODNR injection
files into a valid API number for further processing.

There are various problems with these input strings:
    - they often include extraneous and information
    - they are occasionally a group of API
    - they have no API number but are just descriptive 
    - typos
    
    We handle this by using a hand-checked translation table.  This table
    has the given APIstring, Year and Quarter and returns a valid API with 
    ten digits.
    
"""
import pandas as pd
import os, sys
#from shutil import copyfile
import time

xlatefn = 'xlateAPI.txt'
xlatefnbak = 'xlateAPI_backup.txt'

def fetchXlateDict(fn=xlatefn):
    xdict = {}
    with open(fn) as f:
        f.readline() # ignore header
        for ln in f.readlines():
            lst = ln.split('|')
            xdict[(lst[0],int(lst[1]),int(lst[2]))] = lst[3]
    return xdict

xdict = fetchXlateDict()

def saveSortedXlateDict(fn=xlatefn,bfn=xlatefnbak):
    timestr = time.strftime("%Y%m%d-%H%M%S_") 
    bufn = 'tmp/'+timestr+fn
    try:
        os.rename(bfn,bufn)
    except:
        print('couldnt move backup into tmp!')
    d = pd.read_csv(fn,sep='|')
    os.rename(fn,bfn)   
    out = d.sort_values(by=['APIstr','year','quarter'])
    out.to_csv(fn,sep='|',index=False)
    
    
def is_apistr_numeric(astr):
    # returns true if the first 10 chars are numeric
    if len(astr) <10:
        return False
    try:
        int(astr[0:10])
        # verify that it is not MORE than 10 digits
        if astr[10].isdigit(): 
            return False
        return True
    except:
        return False

def is_potential_multiple(astr):
    try:
        if ('&' in astr) or (',' in astr):
            return True
        else:
            return False
    except:
        print(f'is_potential_multiple error with APIstr: {astr}')
        #sys.exit()

def getAPI10(astr, yr, q,flag_problems=True):
    # First check if we've already identified the API10 to use    
    if (astr,yr,q) in xdict.keys():
        #print(f'{astr} {yr} {q} : {xdict[(astr,yr,q)]}')
        return xdict[(astr,yr,q)]
    if is_potential_multiple(astr):
        if flag_problems:
            print(f'{astr}|{yr}|{q}|API_HERE| Multiple?')
        return f'{astr}|{yr}|{q}|API_HERE| Multiple?'        
    if is_apistr_numeric(astr):
        return astr[0:10]
    else:
        if flag_problems:
            print(f'{astr}|{yr}|{q}|API_HERE|Comment ')
        return f'{astr}|{yr}|{q}|API_HERE|Comment ' 


if __name__ == '__main__':
    print(getAPI10('another test',2009,1))
    saveSortedXlateDict()