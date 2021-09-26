# -*- coding: utf-8 -*-
"""
Created on Sun Jan 28 19:59:42 2018

@author: ogre
"""
#This code is for civil_motor_vehicle and yearend_permanent_population

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import math
class PlotWeather():
    def plotMapBar(self,statistics_csv,img_path,rot=0):
        #read data from csv file
        data = pd.read_csv(statistics_csv)
        #style is the color and shape of the lines
        ax = data.plot(kind="line",fontsize =15,style=["bs-","rd:","go-","^-.",">--"], x="Year",grid=True,markeredgecolor="black",ms=10)
        #set_label(the name of the coord), ex.   ax.set_ylabel("Relative Difference",fontsize=15)
        ax.set_ylabel(list(data)[1],fontsize=15); ax.set_xlabel(list(data)[0],fontsize=15)
        #set the range of coord
        #in plt.xticks: it means the min-x is the first value in col['Year'], the max-x is the last value in col['Year']
        plt.yticks(fontsize=13); plt.xticks(np.arange(data['Year'][0], data['Year'][len(data)-1] + 1, 1), fontsize = 13)
        plt.savefig(img_path,bbox_inches='tight')   

    
#%% plot
plotWeather = PlotWeather()
#plotWeather.plotMapBar("yearend_permanent_population.csv","yearend_permanent_population.pdf")
plotWeather.plotMapBar("civil_motor_vehicle.csv","civil_motor_vehicle.pdf")


    
