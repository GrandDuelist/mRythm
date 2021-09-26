# -*- coding: utf-8 -*-
#%% get average delay of whole year
import pandas as pd
data = pd.read_csv("result/1-traveltime-years/0-year-average-delay.csv")
year_average_delay = data[(data['Average Delay']>= 0) & (data['Average Delay']<=2)]
density_delay = year_average_delay.groupby(['Year','Origin','Destination'])['Average Delay'].mean()
year_delay = density_delay.reset_index()
year_delay.to_csv("data/1-processing/0-density/0-year-density.csv")
#%%  csv to json
import json
json_data_origin = {}
json_data_destination = {}
for ii in xrange(len(year_delay)):
    one = year_delay.iloc[ii]
    year = str(one["Year"]); origin = str(one['Origin']); des = str(one['Destination']); delay = one['Average Delay']
    if year not in json_data_origin: json_data_origin[year] = {}
    if year not in json_data_destination: json_data_destination[year] = {}
    json_data_origin[year][origin] = delay
    json_data_destination[year][des] =  delay
json.dump(json_data_origin,open("data/1-processing/0-density/1-year-origin.json","w"))
json.dump(json_data_destination,open("data/1-processing/0-density/1-year-destination.json",'w'))
#%% generate road network
import os
import json
road_dir = r"J:\My Drive\D-Data\C-Research\C-City-Data\B-Shenzhen\G-Edge\road note"
road_edges = os.path.join(road_dir,"edges.txt");json_edges = os.path.join(road_dir,"roads.json")
with open(road_edges) as fh:
    result = {}
    for line in fh:
        if "LINESTRING" not in line:
            continue
        coordinates = []
        vs = line.split("|")
        vs = [v.strip(" ") for v in vs]
        ogc_fid = vs[0]; startid= int(vs[1]); endid = int(vs[2])
        s = re.search('LINESTRING(.*)', line)
        if s:
            s = s.group(0)
            s= s.replace("LINESTRING(","").replace(")","")
            attrs = s.split(",")
            for attr in attrs:
                lonlat = attr.split(" ")
                coordinates.append([float(lonlat[0]),float(lonlat[1])])
            result[str(ogc_fid)] = {"start_id": startid, "end_id": endid, "coordinates": coordinates}
    json.dump(result,open(json_edges,'w'))

#%% csv to json mapping 
import json
station_dir = r"J:\My Drive\D-Data\C-Research\C-City-Data\B-Shenzhen\C-Subway\3-Lines"
station_file = os.path.join(station_dir,"subway_station_GPS.txt")
station_json = os.path.join(station_dir,"station-locations.json")
mapping = {}
with open(station_file) as fh:
    for line in fh:
        attrs = line.split(",")
        mapping[attrs[1]] = [float(attrs[4]),float(attrs[5])]
json.dump(mapping,open(station_json,'w'))