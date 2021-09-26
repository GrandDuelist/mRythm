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

#%% merge multiple files
import os 
from cpssdk import CPSOS
cpsos = CPSOS()
header = "Origin,Destination,Time,Min,One Quater,Median,Three Quarter,Max,Average"+"\n"

grive = r"J:\My Drive"
data_dir = r"\D-Data\C-Research\C-City-Data\B-Shenzhen\C-Subway\1-Travel Time"
intput = r"\0-spark-output\1-Years-Statistics\0-MinMax\0-station-to-station"
output = r"\1-merged-csv\1-Years-Statistics\0-MinMax\0-station-to-station"

#input_path = grive+data_dir+intput
input_path = r'J:\My Drive\D-Data\C-Research\C-City-Data\B-Shenzhen\D-Taxi\3-Spark-output\0-One-Month\0-MinMax'
#output_path = grive+data_dir+output
one_files = os.listdir(input_path)
for one_file in one_files:
    input_one_file_path = os.path.join(input_path,one_file)
    #output_one_file_path = os.path.join(output_path,one_file)
    cpsos.mergeFiles(input_one_file_path,header=header,extension=".csv")
    
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
import pandas as pd
input_dir = r"J:\My Drive\D-Data\C-Research\C-City-Data\B-Shenzhen\D-Taxi\4-merged-csv\0-OneMonth\0-MinMax"
files  = os.listdir(input_dir)
for one_file in files:
    file_path = os.path.join(input_dir,one_file)
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
rain.to_csv("result/-traveltime/0-rain-average-delay.csv")



#%% merge all csvs
import os
import pandas as pd
import json


csv_input_path = r"J:\My Drive\D-Data\C-Research\C-City-Data\B-Shenzhen\D-Taxi\4-merged-csv\0-OneMonth\0-MinMax"
rain_map = json.load(open("../0-weather/result/1-weather-dates/date-rain-map.json"))
temp_map = json.load(open("../0-weather/result/1-weather-dates/date-temp-map.json"))
wind_map = json.load(open("../0-weather/result/1-weather-dates/date-wind-map.json"))
files  = os.listdir(csv_input_path)
datas = []
for one_file in files:
    year = one_file[:4]; month = one_file[5:7]; day = one_file[8:10]
    date_key =  "-".join([year,month,day])
    if (date_key not in rain_map) or (date_key not in wind_map) or (date_key not in temp_map): continue
    rain = rain_map[date_key]; wind = wind_map[date_key]; temp = temp_map[date_key]
    file_path = os.path.join(csv_input_path,one_file)
    one = pd.read_csv(file_path)
    one['Year'] = pd.Series([year]*len(one))
    one['Month'] = pd.Series([month]*len(one))
    one['Day'] = pd.Series([day]*len(one))
    one['Rain'] = pd.Series([rain]*len(one))
    one['Wind'] = pd.Series([wind]*len(one))
    one['Temp'] = pd.Series([temp]*len(one))
    if len(one) > 0:
        datas.append(one[one['Min']>0])

#%% find the minimum travel time
all_data = pd.concat(datas)
min_travel_time = all_data.groupby(['Origin','Destination'])['Min'].mean()
min_travel_time_data = pd.DataFrame(min_travel_time).reset_index()
min_travel_time_data.rename(columns={'Min': 'Global Min'},inplace=True)
min_travel_time_data.to_csv("result/0-traveltime/0-min-average-delay.csv")

#%% merge all data with minimum
data = pd.merge(all_data,min_travel_time_data,on=['Origin','Destination'])
data['Average Delay'] = (data['Average'] - data['Global Min'])/data['Global Min']
data.to_csv("result/0-traveltime/0-year-average-delay.csv")
year_average_delay = data

#%% group by rain
rain_hour_delay = year_average_delay.groupby(['Rain','Time'])['Average Delay'].mean()
rain_hour_delay
rain_hour_delay = year_average_delay.groupby(['Rain','Time'])['Average Delay'].mean()
rain_hour_delay = rain_hour_delay.reset_index()
rain_hour_delay.to_csv("result/0-traveltime/1-rain-hour-delay.csv")


#%% cummulative possibility -- rain
import math
round_delay = []
for ii in  xrange(len(year_average_delay)):
    delay = year_average_delay.iloc[ii]['Average Delay']
    round_delay.append(math.ceil(delay*10)/10)
year_average_delay['Round Delay'] = pd.Series(round_delay)
#%%
round_delay_count = year_average_delay[['Rain',"Round Delay"]].groupby(['Rain','Round Delay']).size()
round_delay_count = round_delay_count.reset_index(name='count').sort_values(by=['Rain','Round Delay'])
cum = [0]*len(round_delay_count); total = {}; prev= None; cur = None
for ii in xrange(len(round_delay_count)):
    cur = round_delay_count.iloc[ii]['Rain'] 
    if ii == 0: cum[ii] = round_delay_count.iloc[ii]['count'];
    elif cur == prev:
        cum[ii] = round_delay_count.iloc[ii]['count'] + cum[ii-1]   
    elif cur != prev:
        cum[ii] = round_delay_count.iloc[ii]['count']
        total[prev] = cum[ii-1]
    prev = cur
total[prev] = cum[-1]
round_delay_count['cumcount'] = pd.Series(cum)
poses= []; logx = []
for ii in xrange(len(round_delay_count)):
    rain = round_delay_count.iloc[ii]['Rain'] 
    pos = float(round_delay_count.iloc[ii]['cumcount'])/float(total.get(rain))
    logx.append((round_delay_count.iloc[ii]['Round Delay']+1)/3)
    poses.append(pos)
round_delay_count['cumpossibility'] = pd.Series(poses)
round_delay_count['Log Delay'] = pd.Series(logx)
round_delay_count.to_csv("result/0-traveltime/2-round-delay-count-possibility.csv")
