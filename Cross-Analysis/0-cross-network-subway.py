# -*- coding: utf-8 -*-
import os
import json
import pandas as pd
#year_data_delay = pd.read_csv("result/0-traveltime/0-year-average-delay.csv")
data = pd.read_csv(r"../6-Travel-Time-Taxi/result/1-traveltime-years/0-year-average-delay.csv")
#%% plot impact on taxisf
taxi_year_delay = data[(data["Average Delay"]>=0) & (data["Average Delay"] <= 5)]
#taxi_year_delay_all = taxi_year_delay

data = None
#%%
data = pd.read_csv("../5-Travel-Time-Subway/result/1-traveltime-years/0-year-average-delay.csv")
subway_year_delay = data[(data["Average Delay"]>=0) & (data["Average Delay"] <= 2)]
# %%  build origin dest map
def generateMap(start_i,end_i,start_j,end_j):
    mm = {}
    for ii in xrange(start_i,end_i):
        for jj in xrange(start_j,end_j):
            mm["{ii}-{jj}".format(ii=ii,jj=jj)] = True
    return(mm)
def betweenRegions(orig,dest,list_map,total_map):
    for mm in list_map:
        is_orig = mm.get(orig,False)
        is_dest = (not mm.get(dest,False)) and total_map.get(dest,False)
        t = is_orig and is_dest
        if t: break
    return(t)
#%%
params = [(0,5,26,31),(2,7,19,24),(5,10,10,15),(6,11,3,8)]
regions = []; total = {}
for param in params:
    r = generateMap(*param)
    regions.append(r)
    total.update(r)
#%% split taxi to two parts
new_stations = taxi_year_delay[taxi_year_delay.apply(lambda x: betweenRegions(x['Origin'],x['Destination'],regions,total),axis=1)]
old_stations = taxi_year_delay[taxi_year_delay.apply(lambda x: not betweenRegions(x['Origin'],x['Destination'],regions,total),axis=1)]
strategy = "between"
#%% plot the time change in new stations
data = new_stations 
plot_name="new_station_{strategy}_subway.pdf".format(strategy=strategy)
#%%
data = old_stations
plot_name = "old_stations_{strategy}_subway.pdf".format(strategy=strategy)

#%%
data = year_average_delay
plot_name ="all_stations_{strategy}_subway.pdf".format(strategy=strategy)
#%%
from matplotlib.ticker import FuncFormatter
import matplotlib.pyplot as plt
import numpy as np
group_year_by_hour = data.groupby(["Year","Time"])['Average Delay'].mean()
#group_year_by_hour.rename(columns={'Average Delay': 'Hour Average'},inplace=True)
group_year_by_hour_plot = group_year_by_hour.unstack(level=0).reset_index()
ax = group_year_by_hour_plot.plot(kind="line",fontsize =20,style=["bs-","rd:","go-","^-.",">--","rs-"], x="Time",grid=True,markeredgecolor="black",ms=13)
ax.set_ylabel("Delay Ratio",fontsize=20)
ax.legend(loc=0,fontsize=13)#,labels=["Light Rain","No Rain","Heavy Rain"])
ax.set_xlabel("Time of Day",fontsize=20)
plt.xticks(fontsize=15)
#ax.set_ylim((0,0.80))
ax.set_xlim(6,23)
plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
       ncol=2, mode="expand",fontsize=13 ,borderaxespad=0.)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
#plt.legend(loc=0,fontsize=15,mode='expand',borderaxespad=0,ncol=3,bbox_to_anchor=(0,1.14,1,.106))
plt.savefig("plots/lines/{file_name}_taxi_on_taxi".format(file_name=plot_name), bbox_inches='tight')
#%% plot the time change in old stations
stations_plot_data = data[['Year',"Average Delay"]]
stations_plot = stations_plot_data.pivot( columns='Year',values="Average Delay")
ax = stations_plot.plot(kind='box',fontsize=20,showfliers=False)
plt.xticks(fontsize=20)
ax.set_ylabel("Delay Ratio",fontsize=20)
plt.xticks(fontsize=15)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
#ax.set_ylim((0,1))
#plt.legend(loc=0,fontsize=15,mode='expand',borderaxespad=0,ncol=3,bbox_to_anchor=(0,1.14,1,.106))
plt.savefig("plots/box/{file_name}_taxi_on_taxi".format(file_name=plot_name), bbox_inches='tight')
#%% two axis
import matplotlib.ticker as mticker
import pandas as pd
csv_file_name = "taxi-on-taxi-new-high-way-mean.csv"
mean_count = pd.read_csv("data/cross/{csv_file_name}".format(csv_file_name=csv_file_name))
mean_count.set_index("Year")
ax = mean_count['New Station Covered Areas'].plot(style='bs-',markeredgecolor="black",ms=13,label="New High Way Covered Areas")
ax1 = ax.twinx()
ax.set_ylabel("New Station Covered Areas",fontsize=15)
ax.tick_params(labelsize =15)
ax1 = mean_count['Other Areas'].plot(ax=ax1,secondary_y=True,style='rd:',markeredgecolor="black",ms=13)
ax.get_yaxis().set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
ax1.tick_params(labelsize =15)
ax1.set_ylabel("Other Areas",fontsize=15)
ax1.get_yaxis().set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
ax.set_xlabel("Year",fontsize=20)
lines, labels = ax.get_legend_handles_labels()
lines2, labels2 = ax1.get_legend_handles_labels()
ax.legend(lines + lines2, labels + labels2, loc=4,fontsize=13)
ax.xaxis.set_major_locator(mticker.FixedLocator(np.arange(len(mean_count))))
ax.xaxis.set_major_formatter(mticker.FixedFormatter(mean_count.index))
ax.set_xlim(np.array([-0.2, 0.2])+ax.get_xlim())
ax.set_ylim(np.array([-0.05, 0.05])+ax.get_ylim())
ax1.set_ylim(np.array([-0.04, 0])+ax1.get_ylim())
#ax.set_xlim(-1,5)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))

plt.savefig("plots/cross-network-taxi-on-taxi.pdf", bbox_inches='tight')

#%% plot travel time change by month
change_of_month = taxi_year_delay.groupby("Month")['Average Delay'].mean()
#month_change = change_of_month.reset_index()
#month_change = month_change.pivot(index='Month',columns='Year',values='Average Delay')
change_of_month.reset_index().to_csv("data/month-delay-taxi.csv")
#change_of_month.plot(kind='line')
#%%
change_of_month_subway = subway_year_delay[((subway_year_delay['Year']!=2012) & (subway_year_delay['Year']!=2017))].groupby("Month")['Average Delay'].mean()
#month_change = change_of_month.reset_index()
#month_change = month_change.pivot(index='Month',columns='Year',values='Average Delay')
change_of_month_subway.reset_index().to_csv("data/month-delay-subway.csv")
change_of_month_subway.plot(kind='line')
#%% taxi on subway
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import pandas as pd
boxprops = dict(linestyle='-', linewidth=2, color='k')
medianprops = dict(linestyle='-', linewidth=2, color='k')
data = pd.read_csv("data/cross/taxi-on-subway-box.csv")
data.columns = ["TA-B","TA-A","CA-B","CA-A"]
ax = data.plot(kind="box", boxprops=boxprops, medianprops=medianprops,whis=3,legend=False)
plt.plot([None,0.18,0.23],'rd-',ms=12) #['TA-B','TA-A'],
plt.plot([None,None,None,0.2,0.209],'rd-',ms=12) #['CA-B','CA-A'],
plt.xticks(fontsize=26,rotation=0)
plt.yticks(np.arange(0,0.7,0.2),fontsize=28)
#plt.xlabel('Year',fontsize=20)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: int(100*y)))
#plt.legend(loc=0,fontsize=10)
plt.ylabel("Delay Ratio (%)",fontsize=30)
plt.savefig("plots/taxi-on-subway-box.pdf", bbox_inches='tight')
#%% subway on subway
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import pandas as pd
boxprops = dict(linestyle='-', linewidth=2, color='k')
medianprops = dict(linestyle='-', linewidth=2, color='k')
data = pd.read_csv("data/cross/subway-on-subway-box.csv")
data.columns = ["TA-B","TA-A","CA-B","CA-A"]
ax = data.plot(kind="box", boxprops=boxprops, medianprops=medianprops,whis=3,legend=False)
plt.plot([None,0.35,0.16],'rd-',ms=12) #['TA-B','TA-A'],
plt.plot([None,None,None,0.18,0.23],'rd-',ms=12) #['CA-B','CA-A'],
plt.xticks(fontsize=26,rotation=0)
plt.yticks(np.arange(0,0.7,0.2),fontsize=28)
#plt.xlabel('Year',fontsize=20)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: int(100*y)))
#plt.legend(loc=0,fontsize=10)
plt.ylabel("Delay Ratio (%)",fontsize=30)
plt.savefig("plots/subway-on-subway-box.pdf", bbox_inches='tight')
