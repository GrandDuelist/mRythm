# -*- coding: utf-8 -*-
#%% import pandas
import matplotlib.pyplot as plt
import pandas as pd
#%% subway
data = pd.read_csv(r"../5-Travel-Time-Subway/result/1-traveltime-years/0-year-average-delay.csv")
year_average_delay = data[(data["Average Delay"]>=0) & (data["Average Delay"] <= 5)]
#%% taxi
data = pd.read_csv(r"../6-Travel-Time-Taxi/result/1-traveltime-years/0-year-average-delay.csv") 
year_average_delay = data[(data["Average Delay"]>=0) & (data["Average Delay"] <= 5)]
# %%  build origin dest map
orig_map = {}; dest_map = {}
for ii in xrange(2,6):
    for jj in xrange(24,42):
        key = "{ii}-{jj}".format(ii=ii,jj=jj)
        orig_map[key] = True
for jj in xrange(6,13):
    for jj in xrange(9,16):
        key = "{ii}-{jj}".format(ii=ii,jj=jj)
        dest_map[key] = True
        
#%% split taxi to two parts
def betweenTwoAreas(origin,dest):
    return((orig_map.get(origin,False) and dest_map.get(dest,False)) or (orig_map.get(dest,False) and dest_map.get(origin,False)))
    
new_stations = year_average_delay[year_average_delay.apply(lambda x: betweenTwoAreas(x['Origin'],x['Destination']),axis=1)]
old_stations = year_average_delay[year_average_delay.apply(lambda x: not betweenTwoAreas(x['Origin'],x['Destination']),axis=1)]
strategy = "between"

#%% split taxi to two parts
new_map = {}
for ii in xrange(2,13):
    for jj in xrange(9,42):
        key = "{ii}-{jj}".format(ii=ii,jj=jj)
        new_map[key] = True
        
def allNewRegions(origin,dest):
    return(new_map.get(origin,False) or new_map.get(dest,False))
    
new_stations = year_average_delay[year_average_delay.apply(lambda x: allNewRegions(x['Origin'],x['Destination']),axis=1)]
old_stations = year_average_delay[year_average_delay.apply(lambda x: not allNewRegions(x['Origin'],x['Destination']),axis=1)]
strategy = "among"
#%% plot the time change in new stations
data = new_stations 
plot_name="new_station_{strategy}.pdf".format(strategy=strategy)
#%%
data = old_stations
plot_name = "old_stations_{strategy}.pdf".format(strategy=strategy)

#%%
data = year_average_delay
plot_name ="all_stations_{strategy}.pdf".format(strategy=strategy)
#%%
from matplotlib.ticker import FuncFormatter
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
plt.savefig("plots/lines/{file_name}".format(file_name=plot_name), bbox_inches='tight')
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
plt.savefig("plots/box/{file_name}".format(file_name=plot_name), bbox_inches='tight')

#%%
mean_plot = data.groupby(['Year'])['Average Delay'].mean()
mean_plot = mean_plot.reset_index()
#%% 
import numpy as np
csv_file_name = "new_station_between.csv"
mean_plot = pd.read_csv("data/means/{csv_file_name}".format(csv_file_name=csv_file_name))
ax = mean_plot.plot(kind="line",fontsize =20,style=["bs-","rd:","go-","^-.",">--","rs-"], x="Year", grid=True,markeredgecolor="black",ms=13)
ax.set_ylabel("Delay Ratio",fontsize=20)
ax.legend(loc=0,fontsize=13)#,labels=["Light Rain","No Rain","Heavy Rain"])
ax.set_xlabel("Time of Day",fontsize=20)
plt.xticks(np.arange(2013,2018,1),fontsize=15)
#ax.set_ylim((0,0.80))
#ax.set_xlim(6,23)

plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
       ncol=2, mode="expand",fontsize=13 ,borderaxespad=0.)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
#plt.legend(loc=0,fontsize=15,mode='expand',borderaxespad=0,ncol=3,bbox_to_anchor=(0,1.14,1,.106))
plt.savefig("plots/means/{file_name}".format(file_name=plot_name), bbox_inches='tight')

#%% two axis
import matplotlib.ticker as mticker
from matplotlib.ticker import FuncFormatter
import numpy as np
csv_file_name = "new_station_between.csv"
mean_count = pd.read_csv("data/means/{csv_file_name}".format(csv_file_name=csv_file_name))
mean_count.set_index("Year")
mean_count.rename(columns={'New Station Covered Areas':"Covered Areas"},inplace=True)
ax = mean_count['Covered Areas'].plot(style='bs-',markeredgecolor="black",ms=13)
ax1 = ax.twinx()
ax.set_ylabel("Covered Areas",fontsize=35)
ax.tick_params(labelsize =25)
ax1 = mean_count['Other Areas'].plot(ax=ax1,secondary_y=True,style='rd:',markeredgecolor="black",ms=13)
ax.get_yaxis().set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
ax1.tick_params(labelsize =25)
ax1.set_ylabel("Other Areas",fontsize=35)
ax1.get_yaxis().set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
ax.set_xlabel("Year",fontsize=35)
lines, labels = ax.get_legend_handles_labels()
lines2, labels2 = ax1.get_legend_handles_labels()
ax.legend(lines + lines2, labels + labels2, loc=4,fontsize=25)
ax.xaxis.set_major_locator(mticker.FixedLocator(np.arange(len(mean_count))))
ax.xaxis.set_major_formatter(mticker.FixedFormatter(mean_count.index))
ax.set_xlim(np.array([-0.2, 0.2])+ax.get_xlim())
ax.set_ylim(np.array([-0.05, 0.05])+ax.get_ylim())
ax1.set_ylim(np.array([-0.04, 0])+ax1.get_ylim())
#ax.set_xlim(-1,5)
plt.savefig("plots/cross-network-taxi-12.pdf", bbox_inches='tight')

#%% one station
#year_data_delay = pd.read_csv("result/0-traveltime/0-year-average-delay.csv")
data = pd.read_csv(r"../5-Travel-Time-Subway/result/1-traveltime-years/0-year-average-delay.csv")
#%% plot year statistics
year_data_delay = data[(data["Average Delay"]>=0) & (data["Average Delay"] <= 2)]
year_average_delay = year_data_delay
#%% select the ra
qianhaiwan = year_data_delay[year_data_delay['Origin']=="前海湾"]
print(len(qianhaiwan))
#%% plot year box and mean
year_plot_data = qianhaiwan[['Year',"Average Delay"]]
year_plot = year_plot_data.pivot( columns='Year',values="Average Delay")
mean_data = year_plot.mean()
#plot_data= year_plot.sample(frac=0.3)
plot_data = year_plot
#%%

from matplotlib.ticker import FuncFormatter
#box_data = pd.read_csv("data/cross/subway-one-station-box.csv")
box_data = plot_data
xs = range(2013,2018)
#ys = [box_data[v] for v in xs]
#y_axis = [v for _,v in sorted(zip(xs,ys))]
plt.boxplot(box_data,showfliers=False)
#mean_data[2013] = 0.34
#mean_data[2017] = 0.49
#mean_data = pd.read_csv("data/cross/subway-one-station-box-line.csv")
#mean_data = mean_data.sort_index()
#mean_data.sort_index(axis=1)
#ax = box_data.plot(kind='box',position=range(2013,2018),showfliers=False,sym='')
#plt.plot(mean_data.index.tolist(),mean_data.values.tolist(),ls="-",linewidth=3,ms=10,marker="D",c="g")
plt.xticks(fontsize=20)
plt.yticks(fontsize=20)

plt.xlabel('Year',fontsize=20)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
#ax.set_ylabel("Delay Ratio",fontsize=15)
plt.legend(loc=0,fontsize=10)
plt.ylabel("Delay Ratio",fontsize=20)
plt.savefig("plots/year-delay-box-mean-target-station.pdf", bbox_inches='tight')


#%%
import pandas as pd
from matplotlib.ticker import FuncFormatter
import matplotlib.pyplot as plt
box_data = pd.read_csv("data/cross/subway-one-station-box.csv")

boxprops = dict(linestyle='-', linewidth=2, color='k')
medianprops = dict(linestyle='-', linewidth=2, color='k')
ax = box_data.plot(kind="box",positions=range(2013,2018), boxprops=boxprops, medianprops=medianprops)
mean_data = pd.read_csv("data/cross/subway-one-station-box-mean.csv")
ax.plot(mean_data['Year'].tolist(),mean_data['Average Delay Ratio'].tolist(),ls="-",linewidth=3,ms=10,marker="D",c="g")
plt.xticks(fontsize=30)
plt.yticks(fontsize=30)

plt.xlabel('Year',fontsize=35)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))

plt.legend(loc=0,fontsize=10)
plt.ylabel("Delay Ratio",fontsize=35)
plt.savefig("plots/year-delay-box-mean-target-station-12.pdf", bbox_inches='tight')

#%% taxi on taxi
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import pandas as pd
boxprops = dict(linestyle='-', linewidth=2, color='k')
medianprops = dict(linestyle='-', linewidth=2, color='k')
data = pd.read_csv("data/cross/taxi-on-taxi-box.csv")
data.columns = ["TA-B","TA-A","CA-B","CA-A"]
ax = data.plot(kind="box", boxprops=boxprops, medianprops=medianprops,whis=3,legend=False)
plt.plot([None,0.24,0.18],'rd-',ms=12) #['TA-B','TA-A'],
plt.plot([None,None,None,0.25,0.35],'rd-',ms=12) #['CA-B','CA-A'],
plt.xticks(fontsize=26,rotation=0)
plt.yticks(np.arange(0,1.1,0.2),fontsize=28)
#plt.xlabel('Year',fontsize=20)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: int(100*y)))
#plt.legend(loc=0,fontsize=10)
plt.ylabel("Delay Ratio (%)",fontsize=30)
plt.savefig("plots/taxi-on-taxi-box.pdf", bbox_inches='tight')
#%% subway on taxi
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import pandas as pd
boxprops = dict(linestyle='-', linewidth=2, color='k')
medianprops = dict(linestyle='-', linewidth=2, color='k')
data = pd.read_csv("data/cross/subway-on-taxi-box.csv")
data.columns = ["TA-B","TA-A","CA-B","CA-A"]
ax = data.plot(kind="box", boxprops=boxprops, medianprops=medianprops,whis=3,legend=False)
plt.plot([None,0.425,0.436],'rd-',ms=12) #['TA-B','TA-A'],
plt.plot([None,None,None,0.31,0.39],'rd-',ms=12) #['CA-B','CA-A'],
plt.xticks(fontsize=26,rotation=0)
plt.yticks(np.arange(0,1.1,0.2),fontsize=28)
#plt.xlabel('Year',fontsize=20)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: int(100*y)))
#plt.legend(loc=0,fontsize=10)
plt.ylabel("Delay Ratio (%)",fontsize=30)
plt.savefig("plots/subway-on-taxi-box.pdf", bbox_inches='tight')