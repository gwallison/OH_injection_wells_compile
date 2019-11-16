# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
# need both the shp and dbf files for this...

import shapefile as shp  # Requires the pyshp package
import matplotlib.pyplot as plt
sf = shp.Reader("REFER_COUNTY.shp")

plt.figure()
for shape in sf.shapeRecords():
    x = [i[0] for i in shape.shape.points[:]]
    y = [i[1] for i in shape.shape.points[:]]
    plt.plot(x,y,linewidth=0.5, color='black')
plt.show()

