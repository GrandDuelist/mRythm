# -*- coding: utf-8 -*-
import pandas as pd
data = pd.read_csv("data/0-plots/1-traveltime-socialevent/0-social-events-hour-taxi.csv")
data = data[["Time","Weekend","Marathon","Spring Festival Break","National Day Break"]]
data.columns=["Time","Weekend","Marathon","Spring Festival","National Day"]
ax = data.plot(kind="line",fontsize =20,style=["bs-","rd:","go-","^-.",">--","rs-"], x="Time",grid=True,markeredgecolor="black",ms=10)
ax.set_ylabel("Delay Ratio",fontsize=35)
ax.legend(loc=0,fontsize=13)#,labels=["Light Rain","No Rain","Heavy Rain"])
ax.set_xlabel("Time of Day",fontsize=35)
#ax.set_ylim((0.2,0.6))
plt.xticks(np.arange(0, 24, 2))
plt.xticks(fontsize=30)
plt.yticks(fontsize=30)
plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
       ncol=2,fontsize=17, mode="expand", borderaxespad=0.)
ax.set_xlim(6,23)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
plt.savefig("2-plots/1-traveltime-socialevent/social-events-12.pdf", bbox_inches='tight')
#%% plot 