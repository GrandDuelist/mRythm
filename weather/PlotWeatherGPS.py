# -*- coding: utf-8 -*-
import pandas as pd
import matplotlib.pyplot as plt
#%% plot line
input_path = r"result/2-traveltime/0-rain-average-delay.csv"
data = pd.read_csv(input_path)
data
ax = data.plot(kind="line",fontsize =15,style=["bs-","rd:","go-","^-.",">--"], x="Time",grid=True,markeredgecolor="black",ms=10)
ax.set_ylabel("Delay Ratio",fontsize=15)
ax.legend(loc=0,fontsize=15)
ax.set_xlabel("Hours",fontsize=15)
ax.set_ylim((0.5,2.5))
ax.set_xlim((6,24))
plt.savefig("plots/2-traveltime/0-rain-average-delay.pdf", bbox_inches='tight')
#%%
