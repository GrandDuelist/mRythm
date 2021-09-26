# -*- coding: utf-8 -*-
"""
Created on Sun Jan 28 19:59:42 2018

@author: ogre
"""
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import math
class PlotWeather():
    def plotMapBar(self,statistics_csv,img_path,rot=0):
        #read data from csv file
        data = pd.read_csv(statistics_csv)
        #normalization
        for col in list(data):
            if col == "Year":
                continue
            min = data[col][0]
            max = data[col][len(data[col]) - 1]
            data[col] = (data[col] - min)/(max - min)
        #style is the color and shape of the lines
        ax = data.plot(kind="line",fontsize =15,style=["bs-","rd:","go-","^-.",">--","bs-"], x="Year",grid=True,markeredgecolor="black",ms=10)
        #set_label(the name of the coord), ex.   ax.set_ylabel("Relative Difference",fontsize=15)
        ax.set_ylabel("Relative Difference",fontsize=15); ax.set_xlabel(list(data)[0],fontsize=15)
        #set the location of legend, the first parameter in bbox_to anchor is the loc in x-axis, small->large, left->right
        #the second parameter is the loc in y-axis, small->large, down->up
        ax.legend(loc='upper center', bbox_to_anchor=(0.6,1.3),ncol=1,fancybox=True,shadow=True)
        #set the range of coord
        #in plt.xticks: it means the min-x is the first value in col['Year'], the max-x is the last value in col['Year']
        plt.yticks(fontsize=13); plt.xticks(np.arange(data['Year'][0], data['Year'][len(data)-1] + 1, 1), fontsize = 13)
        plt.savefig(img_path,bbox_inches='tight')

    
#%% plot rain
plotWeather = PlotWeather()
plotWeather.plotMapBar("public_transportation.csv","public_transportation.png")



