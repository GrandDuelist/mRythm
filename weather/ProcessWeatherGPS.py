# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import csv
import os 
class WeatherGPS():
    def readFileToDataFrame(self,input_file_path):
        data = pd.read_csv(input_file_path)
        print(data)
        
    def readMultiFilesToDataFrame(self,input_dir):
        files = os.listdir(input_dir)
        for file_name in files:
            file_path = os.path.join(input_dir,file_name)
            self.readFileToDataFrame(file_path)
            
    def travelTimeByHours(self):
        input_dir_path = r"J:\My Drive\D-Data\C-Research\C-City-Data\B-Shenzhen\C-Subway\1-Travel Time\1-merged-csv\20130723.csv"
        self.readFileToDataFrame(input_dir_path)
    
    def travelTimeBetweenLocations(self):
        input_dir_path = r""
    
#%%
weather = WeatherGPS()
weather.travelTimeByHours()


#%% configure drive 
drive =r"/Volumes/GoogleDrive/My Drive"
drive =r"J:/My Drive"
#%%  Average one day
data = pd.read_csv(drive + r"/D-Data/C-Research/C-City-Data/B-Shenzhen/C-Subway/1-Travel Time/1-merged-csv/20130723.csv")
data.groupby("Time")["Average"].mean()
#%% read multiple days
datas = []; names = []; 
mapping ={'20130723': "Light Rain", '20160604': "Shower Rain", '20160609': 'Heavy Rain' ,'20160610': 'Moderate Rain','20160622':'No Rain' ,'20160611': 'Pouring Rain'  }
files  = os.listdir( drive + r"/D-Data/C-Research/C-City-Data/B-Shenzhen/C-Subway/1-Travel Time/1-merged-csv")
for one_file in files:
    file_path = os.path.join(drive + r"/D-Data/C-Research/C-City-Data/B-Shenzhen/C-Subway/1-Travel Time/1-merged-csv",one_file)
    one = pd.read_csv(file_path)
    datas.append(one[one['Min']>0])
    names.append(one_file.replace(".csv",""))
#%% global min
all_data = pd.concat(datas)
#all_data_filtered = all_data[all_data['Min'] > 0]
min_travel_time = all_data.groupby(['Origin','Destination'])['Min'].min()
min_travel_time_data = pd.DataFrame(min_travel_time).reset_index()
min_travel_time_data.rename(columns={'Min': 'Global Min'},inplace=True)
#%%
time_ave = []
for ii in xrange(len(datas)):
    data = datas[ii]; name = names[ii]
    if name == "20160617": continue
    data = pd.merge(data,min_travel_time_data,on=['Origin','Destination'])
    data['Average Delay'] = (data['Average'] - data['Global Min'])/data['Global Min']
    time_ave.append(pd.Series(data.groupby("Time")["Average Delay"].mean(),name=mapping[name]))
rain = pd.DataFrame(time_ave).transpose()
rain.to_csv("result/2-traveltime/0-rain-average-delay.csv")

