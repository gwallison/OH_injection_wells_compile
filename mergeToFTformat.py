# -*- coding: utf-8 -*-
"""
Created on Tue Dec  4 11:20:25 2018

@author: gary.allison

create the injection well dataset

"""

import pandas as pd
import numpy as np
import pandas.api.types as ptypes
import mergeWithMeta

datadir = './sources/'
outdir = './out/'
indir = datadir+'OH_injection/'
pre_proc_out = outdir+'injection_tall_pre.csv'
wide_out = outdir+'injection_wide_format.csv'
tempf = outdir+'temp.csv'

df_pre = pd.read_csv(pre_proc_out)


#### ------  get list of YrQ ---------
yqs = df_pre.YrQ.unique()
yqs.sort()

#### ------ get the metafile and make sure the API lists match
df_wide = mergeWithMeta.makeWholeSet()
assert len(df_wide) == len(df_pre.API10.unique())
df_wide = df_wide.sort_values(by='API10')
assert df_wide.API10.values.sort() == df_pre.API10.unique().sort()
print(f'Len df_wide: {len(df_wide.API10)}, Unique API in df_pre:{len(df_pre.API10.unique())}')


#### -------------- step through each YrQ to get subset to merge ------------
tmp = df_wide.copy()
for yq in yqs:
    d = df_pre[df_pre.YrQ==yq].copy()
    d['chkAPI'] = d.API10.copy()
    # use this drop line for most of the columns of interest
    d = d.drop(['YrQ','Year','Quarter','APIstr','Unnamed: 0'],axis=1)
    # use this drop line to get just companies...
    #d = d.drop(['YrQ','Year','Quarter','APIstr','Unnamed: 0','AltName','Vol_InDist','Vol_OutDist'],axis=1)
    newcol = []
    for c in d.columns:
        if c not in ['API10','chkAPI']:
            newcol.append(c+'_'+yq)
        else:
            newcol.append(c)
    d.columns = newcol
    tmp = pd.merge(tmp,d,how='left',left_on='API10',right_on='API10',validate='1:1')
    #assert tmp.API10 == temp.chkAPI
    tmp = tmp.drop(['chkAPI',],axis=1)
    #dlst.append(tmp)

tmp.to_csv(wide_out,index=False)

