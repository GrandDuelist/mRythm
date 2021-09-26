import pandas as pd
import matplotlib.pyplot as plt

class PlotWeather():
    def plotMapBar(self,statistics_csv,img_path,rot=20):
        data = pd.read_csv(statistics_csv)
        ax = data.plot(kind="bar",edgecolor=['black']*len(data),x=list(data)[0],y=list(data)[1],fontsize = 15)
        ax.set_ylabel(list(data)[1],fontsize=15); ax.set_xlabel(list(data)[0],fontsize=15)
        self.setLegend(ax,data)
        plt.yticks(fontsize=13); plt.xticks(fontsize=13,rotation=rot)
        plt.savefig(img_path,bbox_inches='tight')
    
    def plotMapBarOne(self,statistics_csv,attr,img_path,rot=20):
        data = pd.read_csv(statistics_csv)
        data = data[attr]
        ax = data.plot(kind="bar",edgecolor=['black']*len(data),x=list(data)[0],y=list(data)[1],fontsize = 15)
        ax.set_ylabel(list(data)[1],fontsize=15); ax.set_xlabel(list(data)[0],fontsize=15)
        self.setLegend(ax,data)
        plt.yticks(fontsize=13); plt.xticks(fontsize=13,rotation=rot)
        plt.savefig(img_path,bbox_inches='tight')
        
    def setLegend(self,ax,data):
        handles, labels = ax.get_legend_handles_labels()
        handles = handles * len(data)
        ax.legend(handles,[str(data.iloc[ii][0])+" = "+str(data.iloc[ii][1]) for ii in xrange(len(data))])
#%%
def setLegend(ax,data):
    handles, labels = ax.get_legend_handles_labels()
    handles = handles * len(data)
    ax.legend(handles,[str(data.iloc[ii][0])+" = "+str(data.iloc[ii][1]) for ii in xrange(len(data))])
#%% plot pop
#plot.plotMapBarOne("data.csv",["Year","Population"],"plots/pop.pdf")
csvdata = pd.read_csv("data.csv")
data = csvdata[["Year","Population"]]
ax = data.plot(kind="bar",edgecolor=['black']*len(data),x=list(data)[0],y=list(data)[1],fontsize = 15)
ax.set_ylabel(list(data)[1],fontsize=15); ax.set_xlabel(list(data)[0],fontsize=15)
setLegend(ax,data)
plt.yticks(fontsize=13); plt.xticks(fontsize=13,rotation=0)
plt.savefig(img_path,bbox_inches='tight')
#%%