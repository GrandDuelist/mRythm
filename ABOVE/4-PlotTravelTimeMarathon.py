#%% plot marathon
import pandas as pd
marathon_plot = pd.read_csv("result/1-traveltime-socialevent/1-marathon-hour-delay.csv")
marathon_plot = marathon_plot.groupby(["Marathon","Time"])['Average Delay'].mean().unstack(level=0).reset_index()
ax = marathon_plot.plot(kind="line",fontsize =15,style=["bs-","rd:","go-","^-.",">--","rs-"], x="Time",grid=True,markeredgecolor="black",ms=5)
ax.set_ylabel("Delay Ratio",fontsize=15)
ax.legend(loc=0,fontsize=13)#,labels=["Light Rain","No Rain","Heavy Rain"])
ax.set_xlabel("Time of Day",fontsize=15)
ax.set_ylim((0,0.7))
ax.set_xlim(0,23)
plt.savefig("2-plots/1-traveltime-socialevent/0-marathon-hour-delay.pdf", bbox_inches='tight')
#%%
year_average_delay = data[(data['Average Delay']>= 0) & (data['Average Delay']<=2)]
marathon_2015 = year_average_delay[(year_average_delay['Year']==2015) & (year_average_delay['Month']==1) & (year_average_delay['Day']==1)]
#marathon_2015 = year_average_delay[(year_average_delay['Year']==2015) & (year_average_delay['Month']==10) & (year_average_delay['Day']==28)]
no_marathon_2015 =  year_average_delay[(year_average_delay['Year']==2015) & (year_average_delay['Month']==1) & (year_average_delay['Day']==2)]
marathon_2015["Marathon"] = pd.Series(["Marathon"]*len(marathon_2015))
no_marathon_2015["Marathon"] = pd.Series(["General"] * len(no_marathon_2015))
marathon_data = pd.concat([marathon_2015,no_marathon_2015])
#%%
marathon_plot_data = marathon_data[['Marathon',"Average Delay"]]
marathon_plot = marathon_plot_data.pivot( columns='Marathon',values="Average Delay")
#wind_plot =  pd.read_csv("data/0-plots/0-traveltime-years/0-rain-3-categorites-box-data.csv")
#%% social events
marathon_plot = pd.read_csv("data/0-plots/0-traveltime-years/1-marathon-box-data.csv")
ax = marathon_plot.plot(kind='box')
plt.xticks(fontsize=13)
plt.yticks(fontsize=13)
ax.set_ylabel("Delay Ratio",fontsize=15)
plt.savefig("2-plots/1-traveltime-socialevent/0-marathon-hours-delay-box.pdf", bbox_inches='tight')







#%%
marathon_plot = pd.read_csv("data/0-plots/1-traveltime-socialevent/0-social-events-hour.csv")
#rain_plot = rain_hour_delay.groupby(["Rain","Time"])['Average Delay'].mean().unstack(level=0).reset_index()
ax = marathon_plot.plot(kind="line",fontsize =20,style=["bs-","rd:","go-","^-.",">--","rs-"], x="Time",grid=True,markeredgecolor="black",ms=12)
ax.set_ylabel("Delay Ratio",fontsize=20)
ax.legend(loc=0,fontsize=20)
ax.set_xlabel("Time of Day",fontsize=20)
plt.xticks(fontsize=15)
plt.yticks(fontsize=20)
ax.set_ylim((0.02,0.26))
ax.set_xlim(6,23)
plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
       ncol=2, fontsize=12,mode="expand", borderaxespad=0.)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))

plt.savefig("2-plots/2-traveltime-socialevents/0-socialevents-hour.pdf", bbox_inches='tight')

#%% marathon box plot
marathon_plot = pd.read_csv("data/0-plots/1-traveltime-socialevent/0-social-events-box.csv")
ax = marathon_plot.plot(kind='box',fontsize=20,showfliers=False)
plt.xticks(fontsize=15)
ax.set_ylabel("Delay Ratio",fontsize=20)
plt.xticks(fontsize=15,rotation=20)
plt.yticks(fontsize=20)
#ax.set_ylim((0,1))
#plt.legend(loc=0,fontsize=15,mode='expand',borderaxespad=0,ncol=3,bbox_to_anchor=(0,1.14,1,.106))
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
plt.savefig("2-plots/2-traveltime-socialevents/0-socialevents-box.pdf", bbox_inches='tight')
