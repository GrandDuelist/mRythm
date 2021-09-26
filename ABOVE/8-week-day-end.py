# -*- coding: utf-8 -*-
#%% group by week day and week ends
m = {0: "Weekday", 1: "Weekday", 2: "Weekday", 3:"Weekday", 4: "Weekday", 5: "Weekend", 6:"Weekend"}
from datetime import datetime
day_of_weeks = []
for ii in xrange(len(year_average_delay)):
    one = year_average_delay.iloc[ii]
    year = one['Year']; month = one['Month']; day = one['Day']
    day_of_week = datetime(year=year,month=month,day=day).weekday()
    day_of_weeks.append(m.get(day_of_week))
year_average_delay["Day of Week"] = pd.Series(day_of_weeks, dtype="category")

#%% week day end line data
week_plot = year_average_delay[['Day of Week',"Time",'Average Delay']].groupby(["Day of Week","Time"])['Average Delay'].mean().reset_index()
#week_plot = week_plot
week_plot.to_csv("data/0-plots/2-traveltime-week/0-week-day-end-line.csv")
#%%  plot data -- week day, weekend, hours, line
from matplotlib.ticker import FuncFormatter
import pandas as pd
week_plot = pd.read_csv("data/0-plots/2-traveltime-week/0-week-day-end-line.csv")
week_plot = week_plot.groupby(['Day of Week','Time'])['Average Delay'].mean().unstack(level=0).reset_index()
week_plot.to_csv("data/0-plots/2-traveltime-week/0-week-day-end-line.csv")
#%% 
week_plot = pd.read_csv("data/0-plots/2-traveltime-week/0-week-day-end-line.csv")
ax = week_plot.plot(kind="line",fontsize =20,style=["bs-","rd:","go-","^-.",">--","rs-"], x="Time",grid=True,markeredgecolor="black",ms=13)
ax.set_ylabel("Delay Ratio",fontsize=20)
ax.legend(loc=0,fontsize=13)#,labels=["Light Rain","No Rain","Heavy Rain"])
ax.set_xlabel("Time of Day",fontsize=20)
plt.xticks(fontsize=15)
#ax.set_ylim((0,0.8))
ax.set_xlim(6,23)
plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
       ncol=2,mode="expand",fontsize=13 ,borderaxespad=0.)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
#plt.legend(loc=0,fontsize=15,mode='expand',borderaxespad=0,ncol=3,bbox_to_anchor=(0,1.14,1,.106))
plt.savefig("2-plots/2-traveltime-week/0-wind-hours-delay.pdf", bbox_inches='tight')

#%% week day and end box data
wind_plot_data = year_average_delay[['Wind',"Average Delay"]]
wind_plot = wind_plot_data.pivot( columns='Wind',values="Average Delay")

#%% plot data -- weekday, weekends, box
wind_plot =  pd.read_csv("data/0-plots/0-traveltime-years/1-wind-box-data.csv")
wind_plot.rename(columns={"Whole Gale":"Gale"}, inplace=True)
ax = wind_plot.plot(fontsize=20,kind='box')
plt.xticks(fontsize=20)
ax.set_ylabel("Delay Ratio",fontsize=20)
plt.xticks(fontsize=15,rotation=20)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
#ax.set_ylim((0,1))
#plt.legend(loc=0,fontsize=15,mode='expand',borderaxespad=0,ncol=3,bbox_to_anchor=(0,1.14,1,.106))
plt.savefig("2-plots/1-traveltime-years/0-wind-hours-delay-box-no-outlier.pdf", bbox_inches='tight')

