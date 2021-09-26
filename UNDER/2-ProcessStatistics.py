# -*- coding: utf-8 -*-
import pandas as pd
input_path = r"result\0-traveltime\0-four-month\0-global\0-trip-statistics.csv"

#%%
rain_map = json.load(open("../0-weather/result/1-weather-dates/date-rain-map.json"))
temp_map = json.load(open("../0-weather/result/1-weather-dates/date-temp-map.json"))
wind_map = json.load(open("../0-weather/result/1-weather-dates/date-wind-map.json"))
#%%
statistics = pd.read_csv(input_path)
rains = []; winds = []; temps = []
for ii in xrange(len(statistics)):
    one_record = statistics.iloc[ii]
    year= one_record['Year']; month = one_record['Month'];  day = one_record['Day']
    if len(str(month)) < 2: month = '0'+str(month)
    if len(str(day)) < 2: day = '0'+str(day)
    key = "-".join([str(year),str(month),str(day)])
    rain = rain_map.get(key,None); wind = wind_map.get(key,None); temp= temp_map.get(key,None)
    rains.append(rain); winds.append(wind); temps.append(temp)
print(rains)
statistics['Rain'] = pd.Series(rains)
statistics['Wind'] = pd.Series(winds)
statistics['Temp'] = pd.Series(temps)
statistics.to_csv(r'result\0-traveltime\0-four-month\0-global\0-trip-statistics-weather.csv')
        
