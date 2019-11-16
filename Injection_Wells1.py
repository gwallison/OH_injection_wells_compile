
# coding: utf-8

# # Creating a dataset of Ohio injection wells



import matplotlib.pyplot as plt
import random
import numpy as np
import pandas as pd
import os

# set datadir to the directory that holds the zipfile
datadir = 'c:\MyDocs/sandbox/data/datasets/FracFocus/'
outdir = datadir+'output/'
indir = datadir+'OH_injection/'

tempf = outdir+'temp.csv'
tempf1 = outdir+'temp1.csv'
pre_four = outdir+'pre_four.csv'
# print(os.listdir(indir))
# input files are in three different formats:
# oldest:  tuple (filename,yr,q)
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
# fn_2012 = 'OH_BRINE DISPOSAL FEES FOR 2012 CORRECTED.xlsx'

# bulk of the data are here - first four worksheets are quarters.  Total worksheet ignored
# tuple: (filename,year)
fn_main = [('BRINE DISPOSAL FEES FOR 2013.xlsx',2013),
           ('BRINE DISPOSAL FEES FOR 2014.xlsx',2014),
           ('BRINE DISPOSAL FEES FOR 2015.xlsx',2015),
           ('BRINE DISPOSAL FEES FOR 2016.xlsx',2016), 
           ('BRINE DISPOSAL FEES FOR 2017.xlsx',2017)]
# current files are of a different format.
fn_2018_etc = [('BRINE DISPOSAL FEES FOR 2018.xlsx',2018),
               ('BRINE DISPOSAL FEES FOR 2019.xlsx',2019)]

SWDfn = indir+'Copy of SWD locations - July_2018.xls'
ODNR_permit_pickle = outdir+'ODNR_permit.pkl'
ODNR_injection_pickle = outdir+'ODNR_injection.pkl'

inj_excel = outdir+'Inject_wide.xlsx'


# In[59]:


t = pd.read_pickle(ODNR_injection_pickle)
x = t[t.Owner.str.contains('HUNTER')]
t.to_csv(tempf)


# ## get oldest data

# In[60]:


dlst = []
for fnl in fn_old:
    print(fnl)
    fn = fnl[0]
    yr = fnl[1]
    quar = fnl[2]
#     print(fn,yr,quar)
    d = pd.read_excel(indir+fn,skiprows=5,header=None,usecols=[7,8,10,11],
                     names=['CompanyName','APIstr','Vol','In_Out'])
    d.Vol = d.Vol.where(d.Vol.str.lower().str.strip()!='zero',0)
    d.Vol = pd.to_numeric(d.Vol)
    dIn = d[d.In_Out.str.lower().str[0]=='i']
    dIn = dIn.filter(['CompanyName','APIstr','Vol'])
    dIn.columns = ['CompanyName','APIstr','Vol_InDist']
    
    dOut = d[d.In_Out.str.lower().str[0]=='o']
    dOut = dOut.filter(['APIstr','Vol'])
    dOut.columns = ['APIstr','Vol_OutDist']
    d['Year'] = fnl[1]
    d['Quarter'] = fnl[2]
    
    mg = pd.merge(dIn,dOut,how='outer',left_on='APIstr',right_on='APIstr')
    mg['Year'] = fnl[1]
    mg['Quarter'] = fnl[2]
    
    dlst.append(mg)

old = pd.concat(dlst)
old.to_csv(tempf)


# In[61]:


old.info()


# ## process the 2012 file

# In[62]:


dlst = []
uc1 = [1,2,4,8]
uc2 = [7,8,10,14]
for ws in [0,1,2,3]: # ws 1 is like 'main'; others like 'old'
#     print(ws)
    if ws == 1:
        uc = uc1
    else:
        uc= uc2
#     print(uc)
    d = pd.read_excel(indir+fn_2012,skiprows=6,sheet_name=ws,
                     usecols=uc,header=None,
                     names=['CompanyName','APIstr','Vol_InDist','Vol_OutDist'])
    d = d.dropna(axis=0,subset=['CompanyName'])
    d['Year'] = 2012
    d['Quarter'] = ws+1
    dlst.append(d)
    if ws==1:
        tmp = d

trans2012 = pd.concat(dlst)
trans2012.to_csv(tempf)
tmp.head()


# In[63]:


two = pd.concat([old,trans2012])
two.head()


# 
# ## get main data files

# In[64]:


dlst = []
for fnl in fn_main:
    print(fnl)
    fn = fnl[0]
    yr = fnl[1]
    for ws in [0,1,2,3]: # four quarterly worksheets
        d = pd.read_excel(indir+fn,skiprows=6,sheet_name=ws,
                         usecols=[0,1,2,4,8],header=None,
                         names=['AltName','CompanyName','APIstr','Vol_InDist','Vol_OutDist'])
        d = d.dropna(axis=0,subset=['CompanyName'])
        d['Year'] = yr
        d['Quarter'] = ws+1
#         d.columns= ['AltName','CompanyName','APIstr','Desc',
#                     'Vol_InDist','GrossIn','NetIn','PercRet',
#                     'Vol_OutDist','GrossOut','NetOut','PercRetOut','Comments']
#         print(d.columns)
        dlst.append(d)

main = pd.concat(dlst)
main.to_csv(tempf)


# In[65]:


three = pd.concat([two,main],sort=True)
# out = two.groupby(['APIstr'],as_index=True)['APIstr','Year','Quarter',
#                                                              'CompanyName','Vol_InDist','Vol_OutDist']
three.to_csv(tempf)


# ## get current file

# In[100]:

dlst = []
for fnl in fn_current:
    fn = fnl[0]
    yr = fnl[1]
    #print(fn,yr)
    d = pd.read_excel(indir+fn,skiprows=6,sheet_name=0,
                     usecols=[0,1,2,3,5,9],header=None,
                     names=['QtrStr','AltName','CompanyName','APIstr','Vol_InDist','Vol_OutDist'])
    d = d.dropna(axis=0,subset=['CompanyName'])
    d['Year'] = yr
    d['Quarter'] = d.QtrStr.str[0]
    d = d[d.Quarter != 'Y']
    d = d.filter(['AltName','CompanyName','APIstr','Vol_InDist','Vol_OutDist','Year','Quarter'])
    dlst.append(d)
four = pd.concat(dlst,sort=True)
four = pd.concat([three,four],sort=True)
four.to_csv(tempf)
four.info()


# ## some clean up of the API string and Yr_Q
# 
# 

# In[101]:


four.APIstr = four.APIstr.astype('str')  # make sure all are strings

# First create some flags base on status of APIstr
four['NoAPIstr'] = four.APIstr.str.strip()==''
print(f'Number of records with no APIstring: {four.NoAPIstr.sum()}')
four.APIstr = np.where(four.NoAPIstr,'No API string recorded',four.APIstr)

four['API_non_numeric'] = ~four.APIstr.str[:5].str.isnumeric().astype('bool') 
print(f'Number of records that are non-numeric: {four.API_non_numeric.sum()}')

four['MultipleNumericAPI'] = four.APIstr.str.contains('&') & ~four.API_non_numeric
# four['tmp'] = four.APIstr.astype('str') + '&junk'
# four.tmp = four.tmp.str.split('&').str.get(0).str[0:10]  # grab first 10 char before the &
print(f'Number of records with multiple numeric API: {four.MultipleNumericAPI.sum()}')

four['temp1'] = four.APIstr.astype('str') + '/' # cover those few API without /
four.temp1 = four.temp1.str.replace('SWIW','/SWIW')
four.temp1 = four.temp1.str.split('/').str.get(0)


four['API10'] = four.APIstr  # just use APIstr for non-numeric
cond = ~four.NoAPIstr & ~four.API_non_numeric & ~ four.MultipleNumericAPI
four.API10 = np.where(cond,four.temp1,four.API10)

# one more tweek
cond2 = four.API10.str.len() >15
cond3 = ~four.API_non_numeric
cond4 = cond2 & cond3
four.MultipleNumericAPI = np.where(cond4,True,four.MultipleNumericAPI)
four.API10  = np.where(four.MultipleNumericAPI,four.APIstr.str[0:10],four.API10) # fix API10 for the multiple API records

four.API10 = four.API10.astype('str')
four.to_csv(tempf)
# four[four.MultipleNumericAPI].head(20)


# In[102]:



four.Year = four.Year.astype('str')
four.Quarter = four.Quarter.astype('str')
four['YrQ'] = four.Year+'Q'+four.Quarter
four = four.drop(['Year','Quarter'],axis=1)
four = four.filter(['API10','MultipleNumericAPI','APIstr','CompanyName','AltName','YrQ','Vol_InDist','Vol_OutDist'])
# four[four.API10.str.contains('Trum')].head(20)
four.to_csv(tempf)


# ## Changing by hand: non_numeric API to a representative API

# In[103]:


cond1 = four.API10.str.contains('Ashtabula')
four.API10 = np.where(cond1,'3400723262',four.API10)

cond1 = four.API10.str.lower().str.contains('trum')
#len(four[cond1])
four.API10 = np.where(cond1,'3415521893',four.API10)

cond1 = four.API10.str.lower().str.contains('stark')
len(four[cond1])
four.API10 = np.where(cond1,'3415121920',four.API10)

cond1 = four.API10.str.contains('4439/SWIW')
len(four[cond1])
four.API10 = np.where(cond1,'3411924439',four.API10)

cond1 = four.API10.str.contains('34009237610000')
len(four[cond1])
four.API10 = np.where(cond1,'3400923761',four.API10)


four.to_csv(pre_four)


# ## how well does API10 map on to APIstr (the full string)?

# In[104]:


a10s = list(four.API10.unique())
api10 = []
astr = []
for a in a10s:
    api10.append(a)
    astr.append(list(four.APIstr[four.API10==a].unique()))
tmp = pd.DataFrame({'api10':api10,'APIstr':astr})
tmp.to_csv(tempf)


# ## Get SWD list
# 

# In[111]:


SWD_df = pd.read_excel(SWDfn)
SWD_df['API10'] = SWD_df['API #'].astype('str').str[0:10]
SWD_df.API10 = SWD_df.API10.astype('str')
SWD_df.columns = ['API','Owner','WellName','County','Township','Latitude','Longitude','WellStatus','API10']
# SWD_df.head()
apis = pd.DataFrame({'API10':four.API10.unique()})
tmp = pd.merge(SWD_df,apis,how='outer',left_on='API10',right_on='API10',indicator=True)
tmp.to_csv(tempf)
tmp.head()
col0  = tmp[tmp['_merge']=='both']
col0 = col0.drop(['_merge'],axis=1) #,'API'],axis=1)   #######
col0['meta source'] = 'SWD_list_july_2018'
col0.to_csv(tempf)
# # not matched yet
colnot = tmp[tmp['_merge']=='right_only']
colnot = colnot.filter(['API10'],axis=1)


# ## Examine APIs so we can fetch metadata

# In[106]:


# apis = pd.DataFrame({'API10':four.API10.unique()})

# first try to match from the injection well dataset
ODNRi = pd.read_pickle(ODNR_injection_pickle)
ODNRi.API10 = ODNRi.API10.astype('str')
mg_apii = pd.merge(ODNRi,colnot,how='outer',left_on='API10',right_on='API10',indicator=True)
col1 = mg_apii[mg_apii['_merge']=='both']
col1 = col1.drop(['_merge','API'],axis=1)
col1['meta source'] = 'ODNR_injection'
# not matched yet
col2 = mg_apii[mg_apii['_merge']=='right_only']
col2 = col2.filter(['API10'],axis=1)

# try to match the rest with the permit dataset
ODNR = pd.read_pickle(ODNR_permit_pickle)
ODNR.API10 = ODNR.API10.astype('str')
mg_api = pd.merge(ODNR,col2,how='outer',left_on='API10',right_on='API10',indicator=True)
col3 = mg_api[mg_api['_merge']=='both']
col3 = col3.drop(['_merge','API'],axis=1)
col3['meta source'] = 'ODNR_permit'
col3.columns = ['County','Owner','Township','PermitDate','WellName','WellNumber','Latitude','Longitude','API10','meta source']

col4 = mg_api[mg_api['_merge']=='right_only']
col4 = col4.filter(['API10'],axis=1)
col4['meta source'] = 'No_Match'

col5 = pd.concat([col0,col1,col3,col4],sort=True)
col5.to_csv(tempf1)


# ## Reorganize so data are in FT format

# In[140]:


yqs = four.YrQ.unique()
yqs.sort()
df_wide = apis.copy()
df_wide['chk_API10'] = df_wide.API10.copy()
dlst = [df_wide]
for yq in yqs:
    d = four[four.YrQ==yq].copy()
    d['chkAPI'] = d.API10.copy()
    d = d.drop(['YrQ'],axis=1)
    newcol = []
    for c in d.columns:
        if c != 'API10':
            newcol.append(c+' '+yq)
        else:
            newcol.append(c)
    d.columns = newcol
    print(d.duplicated)
    tmp = pd.merge(df_wide,d,how='left',left_on='API10',right_on='API10',validate='1:m')
    tmp = tmp.drop(['API10'],axis=1)
    dlst.append(tmp)

df_wide = pd.concat(dlst,axis=1)    
# df_wide.API10 = df_wide.API10.astype('str')
# col5.API10 = col5.API10.astype('str')
df_wide.to_csv(tempf)
# df_wide = df_wide.filter(['API10','APIstr 2010Q3'],axis=1)
col5 = col5.filter(['API10'],axis=1)
# col5 = col5.sort_values(by='API10')
big = pd.merge(col5,df_wide,on='API10',how='outer',validate='1:m',indicator=True)
big.head()
# df_wide.head()


# In[148]:


print(f'{len(df_wide.API10)}, {len(df_wide.API10.unique())}')
print(f'{len(col5.API10)}, {len(col5.API10.unique())}')
df_wide.to_csv(tempf)


# In[80]:


col5.info()


# In[126]:


big.to_excel(inj_excel)


# In[35]:


four.plot('YrQ','Vol_InDist',style='o')

