# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt
from matplotlib import rcParams
rcParams['font.family'] = 'Times New Roman'
#%%
marathon_plot = pd.read_csv("data/0-plots/1-traveltime-socialevent/0-social-events-hour.csv")
#rain_plot = rain_hour_delay.groupby(["Rain","Time"])['Average Delay'].mean().unstack(level=0).reset_index()
ax = marathon_plot.plot(kind="line",fontsize =20,style=["bs-","rd:","go-","^-.",">--","rs-"], x="Time",grid=True,markeredgecolor="black",ms=12)
ax.set_ylabel("Delay Ratio",fontsize=35)
ax.legend(loc=0,fontsize=20)
ax.set_xlabel("Time of Day",fontsize=35)
plt.xticks(fontsize=30)
plt.yticks(fontsize=30)
ax.set_ylim((0.02,0.26))
ax.set_xlim(6,23)
plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
       ncol=2, fontsize=21,mode="expand", borderaxespad=0.)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))

plt.savefig("2-plots/2-traveltime-socialevents/0-socialevents-hour.pdf", bbox_inches='tight')

#%% marathon box plot
marathon_plot = pd.read_csv("data/0-plots/1-traveltime-socialevent/0-social-events-box.csv")
ax = marathon_plot.plot(kind='box',fontsize=20,showfliers=False)
plt.xticks(fontsize=20)
ax.set_ylabel("Delay Ratio",fontsize=20)
plt.xticks(fontsize=17,rotation=20)
plt.yticks(fontsize=20)
#ax.set_ylim((0,1))
#plt.legend(loc=0,fontsize=15,mode='expand',borderaxespad=0,ncol=3,bbox_to_anchor=(0,1.14,1,.106))
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
plt.savefig("2-plots/2-traveltime-socialevents/0-socialevents-box.pdf", bbox_inches='tight')