# -*- coding: utf-8 -*-
"""
Created on Sun Jan 28 15:58:44 2018
@author: ogre
"""
import collections
import csv
class ProcessWeather():
    def dumpMap(self,output_file,out_map,keys):
        with open(output_file,'w') as outputfile:
            for k in keys:
                line = ",".join([str(k),str(out_map[k])])+"\n"
                outputfile.write(line)
    def dumpMapHorizon(self,output_file,out_map,keys):
        with open(output_file,'w') as outputfile:
            outputfile.write(",".join([str(k) for k in keys])+"\n")
            outputfile.write(",".join([str(out_map[k]) for k in keys])+"\n")
    
    def dumpMapArray(self,output_file,out_map,keys):
        l = max([len(out_map[k]) for k in keys])
        with open(output_file,'w') as outputfile:
            outputfile.write(",".join(keys)+"\n")
            for ii in xrange(l):
                one_line = []
                for k in keys:
                    if ii < len(out_map[k]):
                        one_line.append(str(out_map[k][ii]))
                    else:
                        one_line.append("")
                out_line = ",".join(one_line)
                outputfile.write(out_line+"\n")
    
    def getRainKey(self,instr,rains,rain_keys):
        key = rain_keys[0]
        for ii in xrange(len(rains)):
            w = rains[ii]
            if w in instr:
                key = rain_keys[ii]
        return(key)

    def getTempKey(self,instr,temps,temp_keys):
        key = 20
        try:
            temp = int(instr[:2])
            for ii in xrange(len(temps)-1,-1,-1):
                if temp < temps[ii]:
                    key = temp_keys[ii]
        except:
            pass
        return(key)
    
    def getWindKey(self,instr,winds,wind_keys):
        key = wind_keys[0]
        for ii in xrange(len(winds)):
            wind = winds[ii]
            if wind in instr:
                key = wind_keys[ii]
        return(key)
        
    def processStatistics(self,input_file,output_file_dir):
        rains = ["晴","小雨","阵雨","中雨","大雨","暴雨"]
        rain_keys = ["No Rain","Light Rain","Shower Rain","Moderate Rain","Heavy Rain","Pouring Rain"]
        temps = [20,25,30,35,40]
        temp_keys = ["<20","20~25","25~30","30~35","35~40"]
        winds = ['4 less','4','5','6','7','8','9','10']
        wind_keys = ["Gentle Breeze","Moderate Breeze","Fresh Breeze","Strong Breeze","Moderate Gale","Fresh Gale","Strong Gale","Whole Gale"]
        rain_map = collections.defaultdict(int)
        temp_map = collections.defaultdict(int)
        wind_map = collections.defaultdict(int)
        
        with open(input_file) as csvfile:
            for row in csvfile:
                [city,date,rain,temp,wind] = row.split(",")
                rain_map[self.getRainKey(rain,rains,rain_keys)] += 1
                temp_map[self.getTempKey(temp,temps,temp_keys)] += 1
                wind_map[self.getWindKey(wind,winds,wind_keys)] += 1
        
        self.dumpMap(output_file_dir+"/rain.csv",rain_map,rain_keys)
        self.dumpMap(output_file_dir+"/temp.csv",temp_map,temp_keys)
        self.dumpMap(output_file_dir+"/wind.csv",wind_map,wind_keys)
    
    
    
    def processDates(self,input_file,output_file_dir):
        rains = ["晴","小雨","阵雨","中雨","大雨","暴雨"]
        rain_keys = ["No Rain","Light Rain","Shower Rain","Moderate Rain","Heavy Rain","Pouring Rain"]
        temps = [20,25,30,35,40]
        temp_keys = ["<20","20~25","25~30","30~35","35~40"]
        winds = ['4 less','4','5','6','7','8','9','10']
        wind_keys = ["Gentle Breeze","Moderate Breeze","Fresh Breeze","Strong Breeze","Moderate Gale","Fresh Gale","Strong Gale","Whole Gale"]
        rain_map = collections.defaultdict(list)
        temp_map = collections.defaultdict(list)
        wind_map = collections.defaultdict(list)
        
        with open(input_file) as csvfile:
            for row in csvfile:
                [city,date,rain,temp,wind] = row.split(",")
                rain_map[self.getRainKey(rain,rains,rain_keys)].append(date)
                temp_map[self.getTempKey(temp,temps,temp_keys)].append(date)
                wind_map[self.getWindKey(wind,winds,wind_keys)].append(date)
        self.dumpMapArray(output_file_dir+"/rain-dates.csv",rain_map,rain_keys)
        self.dumpMapArray(output_file_dir+"/temp-dates.csv",temp_map,temp_keys)
        self.dumpMapArray(output_file_dir+"/wind-dates.csv",wind_map,wind_keys)
        
#%% weather statistics
weather = ProcessWeather()
weather.processStatistics("data/0-weather/weather-records.csv","result/0-weather-5-years")
#%%
weather = ProcessWeather()
weather.processDates("data/0-weather/weather-records.csv","result/1-weather-dates")
#%% map weather-dates.csv to date-weather.json
import json
def readMap(file_path,map_data,save_filepath=None):
    csv_data = pd.read_csv(file_path)
    for name in list(csv_data):
        for day in csv_data[name]:
            if type(day) is float: 
                continue
            dates = day.split("/")
            if len(dates[0]) < 2: dates[0] ="0"+dates[0]
            if len(dates[1]) < 2: dates[1] = "0"+dates[1]
            key = dates[2]+"-"+dates[0]+"-"+dates[1]
            map_data[key] = name
    if save_filepath: json.dump(map_data,open(save_filepath,'w'))
    
#%% generate rain map
rain_map = {}; wind_map = {}; temp_map = {}; 
readMap("result/1-weather-dates/rain-dates.csv",rain_map,"result/1-weather-dates/date-rain-map.json")
readMap("result/1-weather-dates/wind-dates.csv",wind_map,"result/1-weather-dates/date-wind-map.json")
readMap("result/1-weather-dates/temp-dates.csv",temp_map,"result/1-weather-dates/date-temp-map.json")