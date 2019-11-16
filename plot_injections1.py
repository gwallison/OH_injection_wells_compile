# -*- coding: utf-8 -*-
"""
Created on Tue Dec  4 14:19:29 2018

@author: BMansfield
"""
import shapefile as shp
import pandas as pd
import numpy as np
#from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import mergeWithMeta

datadir = './sources/'
outdir = './out/'
pre_proc_out = outdir+'injection_tall_pre.csv'
tempf = outdir+'temp.csv'

tall = mergeWithMeta.addLatLonToTall()
tall.to_csv(tempf)

dfpl = tall[tall.YrQ=='2019Q4'].copy()
dfpl['lIn'] = np.log(dfpl.Vol_InDist + 1)
dfpl.plot.scatter('Longitude','Latitude',c='lIn',colormap='plasma')


#need both the shp and dbf files for this...

import shapefile as shp  # Requires the pyshp package
import matplotlib.pyplot as plt
sf = shp.Reader("REFER_COUNTY.shp")

plt.figure()
for shape in sf.shapeRecords():
    x = [i[0] for i in shape.shape.points[:]]
    y = [i[1] for i in shape.shape.points[:]]
    plt.plot(x,y,linewidth=0.5, color='black')
plt.show()




