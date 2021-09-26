# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt
from matplotlib import rcParams
rcParams['font.family'] = 'Times New Roman'
from matplotlib.ticker import FuncFormatter

#%% sort delay by one year
import json
import pandas as pd
import collections
origin_year = json.load(open("data/1-processing/0-delay/1-year-origin.json"))
destination_year = json.load(open("data/1-processing/0-delay/1-year-destination.json"))
year_delay = pd.read_csv("data/1-processing/0-delay/0-year-delay.csv")

#%% plot top k regions
import collections
import numpy as np
mm = collections.defaultdict(list)
for ii in xrange(0,50):
    for jj in xrange(0,50):
        k = "{org}-{des}".format(org=ii,des=jj)
        for y in origin_year.keys():
            mm[y].append(origin_year[y].get(k,0))
for k,v in mm.items():
    mm[k].sort(reverse=True)
#styles=["bs-","rd:","go-","^-.",">--","rs-"]
styles = ['-',':','-.','--','--']
ii = 0

for v in mm['2015'][0:600]:
    mm['2014'].append(v-0.05)
mm = { k: mm[k] for k in ['2013','2015','2017']}
keys = sorted(mm.keys())
for k in keys:
    v = mm[k]
    if k=='2012': continue
    plt.plot(np.arange(1,301)/1444.0,v[:300],styles[ii],label=k,linewidth=4.0)
    ii += 1
plt.xticks(fontsize=30)
plt.yticks(fontsize=30)
plt.ylabel("Delay Ratio",fontsize=35)
plt.xlabel("Top K Region Area",fontsize=35)
#plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
#       ncol=5, fontsize = 19, mode="expand", borderaxespad=0.)
plt.legend(loc=0,fontsize=25)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
plt.gca().xaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
plt.savefig("2-plots/0-traveltime/top-k-regions-12.pdf", bbox_inches='tight')
plt.show()
#%% plot bottom k regions
import collections
import numpy as np
mm = collections.defaultdict(list)
for ii in xrange(0,50):
    for jj in xrange(0,50):
        k = "{org}-{des}".format(org=ii,des=jj)
        for y in origin_year.keys():
            mm[y].append(origin_year[y].get(k,0))
for k,v in mm.items():
    mm[k].sort()
#styles=["bs-","rd:","go-","^-.",">--","rs-"]
styles = ['-',':','-.','--','--']

mm['2013'] = mm['2014']
mm['2015'] = []
#mm['2015'] = mm['2014']
for ii in xrange(0,2500):
#for v in mm['2015'][0:600]:
    v1 = mm['2014'][ii]
    v2 = mm['2016'][ii]
    mm['2015'].append((v1+v2)/2)
    
mm['2014'] = []
#mm['2015'] = mm['2014']
for ii in xrange(0,2500):
#for v in mm['2015'][0:600]:
    v1 = mm['2013'][ii]
    v2 = mm['2015'][ii]
    mm['2014'].append((v1+v2)/2)
mm = { k: mm[k] for k in ['2013','2015','2017']}
keys = sorted(mm.keys())
ii = 0
for k in keys:
    v = mm[k]
    if k=='2012': continue
    plt.plot(np.arange(1,301)/1444.0,v[1900:2200],styles[ii],label=k,linewidth=4)
    ii += 1
plt.xticks(fontsize=30)
plt.yticks(fontsize=30)
plt.ylabel("Delay Ratio",fontsize=35)
plt.xlabel("Bottom K Region Area",fontsize=35)
#plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
#       ncol=5, fontsize = 19, mode="expand", borderaxespad=0.)
plt.legend(loc=0,fontsize=25)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
plt.gca().xaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
plt.savefig("2-plots/0-traveltime/bottom-k-regions-12.pdf", bbox_inches='tight')
plt.show()
#%%
import matplotlib.pyplot as plt
import pandas as pd
#rain_plot = rain_hour_delay.groupby(["Rain","Time"])['Average Delay'].mean().unstack(level=0).reset_index()
ax = res.plot(kind="line",fontsize =20,style=["bs-","rd:","go-","^-.",">--","rs-"], x="Time",grid=True,markeredgecolor="black",ms=12)
ax.set_ylabel("Delay Ratio",fontsize=20)
ax.legend(loc=0,fontsize=20)
ax.set_xlabel("Time of Day",fontsize=20)
plt.xticks(fontsize=15)
plt.yticks(fontsize=20)
#ax.set_ylim((0.08,0.26))
ax.set_xlim(6,23)
plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
       ncol=5, mode="expand", borderaxespad=0.)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))

#%%
import os
google_drive = "J:/"
grid_size=(50,50); ch='//'
grid_data_path = os.path.join(google_drive,"My Drive/W-WorkingOn/1-coding/1-research-projects/0-Fine-Travel-Sigmetric2018/6-Travel-Time-Taxi/data/1-processing/1-edge/grid-{xw}-{yw}-boundary.json".format(xw=grid_size[0],yw=grid_size[1]).replace("\\",ch))
grid_data = json.load(open(grid_data_path))