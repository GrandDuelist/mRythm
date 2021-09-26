# -*- coding: utf-8 -*-
"""
Created on Sun Jan 28 19:59:42 2018

@author: ogre
"""
import matplotlib.pyplot as plt
import pandas as pd
class PlotWeather():
    def plotMapBar(self,statistics_csv,img_path,rot=20):
        data = pd.read_csv(statistics_csv)
        ax = data.plot(kind="bar",edgecolor=['black']*len(data),x=list(data)[0],y=list(data)[1],fontsize = 15)
        ax.set_ylabel(list(data)[1],fontsize=15); ax.set_xlabel(list(data)[0],fontsize=15)
        self.setLegend(ax,data)
        plt.yticks(fontsize=13); plt.xticks(fontsize=13,rotation=rot)
        plt.savefig(img_path,bbox_inches='tight')
        
    def setLegend(self,ax,data):
        handles, labels = ax.get_legend_handles_labels()
        handles = handles * len(data)
        ax.legend(handles,[str(data.iloc[ii][0])+" = "+str(data.iloc[ii][1]) for ii in xrange(len(data))])
    
#%% plot rain
plotWeather = PlotWeather()
plotWeather.plotMapBar("result/0-weather/rain.csv","plots/0-weather/rain.pdf")
#%%
plotWeather.plotMapBar("result/0-weather/temp.csv","plots/0-weather/temp.pdf",rot = 0)
#%%
plotWeather = PlotWeather()
plotWeather.plotMapBar("result/0-weather/wind.csv","plots/0-weather/wind.pdf")

#%% plot 

#%% rain weather
grive = ""; 
input_path = r"G:\OneDrive - Rutgers University\D-Workspace\pv-gps\training-days.csv"
data = pd.read_csv(input_path)
ax = data.plot(kind="line",fontsize =15,style=["bs-","rd:","go-","^-.",">--"], x="days",grid=True,markeredgecolor="black",ms=10)
ax.set_ylabel("MAPE",fontsize=15)
ax.legend(loc=0,fontsize=15)
ax.set_xlabel("Days",fontsize=15)
ax.set_ylim((0,0.8))
plt.savefig("training-days-private.pdf", bbox_inches='tight')

