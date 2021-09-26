# -*- coding: utf-8 -*-
#%% merge csv 
import os 
from cpssdk import CPSOS
cpsos = CPSOS()
header = "Origin,Destination,Time,Min,One Quater,Median,Three Quarter,Max,Average"+"\n"
input_path = r'J:\My Drive\D-Data\C-Research\C-City-Data\B-Shenzhen\D-Taxi\3-Spark-output\1-Taxi-OD\0-Minmax-four-year\0-station-to-station'
#output_path = grive+data_dir+output
one_files = os.listdir(input_path)
for one_file in one_files:
    input_one_file_path = os.path.join(input_path,one_file)
    #output_one_file_path = os.path.join(output_path,one_file)
    cpsos.mergeFiles(input_one_file_path,header=header,extension=".csv")

#%% merge all csvs
import os
import pandas as pd
import json

csv_input_path = r"J:\My Drive\D-Data\C-Research\C-City-Data\B-Shenzhen\D-Taxi\4-merged-csv\1-FourYears\0-MinMax"
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
#%% fitler by data type
all_data = all_data[pd.to_numeric(all_data['One Quater'],errors='coerce').notnull()]
# get column name
column_names = list(all_data)
all_data['Normalized Min'] = (all_data['Min']+all_data['One Quater'])/2
all_data = all_data[pd.to_numeric(all_data['Normalized Min'],errors='coerce').notnull()]
all_data['Normalized Min'] = all_data['Normalized Min'].astype(float)
min_travel_time = all_data.groupby(['Origin','Destination'])["Normalized Min"].mean()
min_travel_time_data = pd.DataFrame(min_travel_time).reset_index()
min_travel_time_data.rename(columns={'Normalized Min': 'Global Min'},inplace=True)
min_travel_time_data.to_csv("result/1-traveltime-years/0-min-average-delay.csv")
#%% merge all data with minimum
data = pd.merge(all_data,min_travel_time_data,on=['Origin','Destination'])
data['Average Delay'] = (data['Average'] - data['Global Min'])/data['Global Min']
data.to_csv("result/1-traveltime-years/0-year-average-delay.csv")
#%% calculate average delay
year_average_delay = data[(data["Average Delay"]>=0) & (data["Average Delay"] <= 2)]
rain_hour_delay = year_average_delay.groupby(['Rain','Time'])['Average Delay'].mean()
rain_hour_delay = rain_hour_delay.reset_index()
rain_hour_delay.to_csv("result/1-traveltime-years/1-rain-hour-delay.csv")

#%% calculate average delay -- wind
year_average_delay = data[(data["Average Delay"]>=0) & (data["Average Delay"] <= 2)]
wind_hour_delay = year_average_delay.groupby(['Wind','Time'])['Average Delay'].mean()
wind_hour_delay = wind_hour_delay.reset_index()
wind_hour_delay.to_csv("result/1-traveltime-years/1-wind-hour-delay.csv")

#%% calculate average delay -- temp
year_average_delay = data[(data["Average Delay"]>=0) & (data["Average Delay"] <= 2)]
temp_hour_delay = year_average_delay.groupby(['Temp','Time'])['Average Delay'].mean()
temp_hour_delay = temp_hour_delay.reset_index()
temp_hour_delay.to_csv("result/1-traveltime-years/1-temp-hour-delay.csv")

#%% cummulative possibility -- rain
import math
round_delay = []

year_average_delay_back = year_average_delay
year_average_delay =  year_average_delay.sample(frac=0.1)
#year_average_delay = year_data_delay
#%%
for ii in  xrange(len(year_average_delay)):
    delay = year_average_delay.iloc[ii]['Average Delay']
    round_delay.append(math.ceil(delay*1000)/1000)
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
#    logx.append((round_delay_count.iloc[ii]['Round Delay'])
    poses.append(pos)
round_delay_count['cumpossibility'] = pd.Series(poses)
#round_delay_count['Log Delay'] = pd.Series(logx)
round_delay_count.to_csv("result/1-traveltime-years/2-round-delay-count-possibility.csv")

#%% cum plot data ratio
import pandas as pd
import math
data = pd.read_csv("result/1-traveltime-years/0-year-average-delay.csv")
round_delay = []
year_average_delay =  data.sample(frac=0.3)
for ii in  xrange(len(year_average_delay)):
    delay = year_average_delay.iloc[ii]['Average Delay']
    round_delay.append(math.ceil(delay*100)/100)
year_average_delay['Round Delay'] = pd.Series(round_delay)
#%% cum plot data wind
round_delay_count = year_average_delay[['Wind',"Round Delay"]].groupby(['Wind','Round Delay']).size()
round_delay_count = round_delay_count.reset_index(name='count').sort_values(by=['Wind','Round Delay'])
cum = [0]*len(round_delay_count); total = {}; prev= None; cur = None
for ii in xrange(len(round_delay_count)):
    cur = round_delay_count.iloc[ii]['Wind'] 
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
    rain = round_delay_count.iloc[ii]['Wind'] 
    pos = float(round_delay_count.iloc[ii]['cumcount'])/float(total.get(rain))
#    logx.append((round_delay_count.iloc[ii]['Round Delay'])
    poses.append(pos)
round_delay_count['cumpossibility'] = pd.Series(poses)
#round_delay_count['Log Delay'] = pd.Series(logx)
round_delay_count.to_csv("result/1-traveltime-years/2-round-delay-count-possibility-wind-sample-30.csv")



#%% append day of week to the data
m = {0: "Monday", 1: "Tuesday", 2: "Wednesday", 3:"Thursday", 4: "Friday", 5: "Saturday", 6:"Sunday"}
m = {0: "Weekday", 1: "Weekday", 2: "Weekday", 3:"Weekday", 4: "Weekday", 5: "Weekend", 6:"Weekend"}
from datetime import datetime
day_of_weeks = []
for ii in xrange(len(year_average_delay)):
    one = year_average_delay.iloc[ii]
    year = one['Year']; month = one['Month']; day = one['Day']
    day_of_week = datetime(year=year,month=month,day=day).weekday()
    day_of_weeks.append(m.get(day_of_week))
year_average_delay["Day of Week"] = pd.Series(day_of_weeks, dtype="category")

#%% box plot data from different temporature
group_temp = []
mm = {"<20":"Cold", "35~40":"Hot"}
for ii in xrange(len(year_average_delay)):
    one =  year_average_delay.iloc[ii]
    temp = one['Temp']
    group_temp.append(mm.get(temp,"General"))
year_average_delay['Temp Group'] = pd.Categorical(group_temp,['Cold',"General",'Hot'])
