# -*- coding: utf-8 -*-
import json
google_drive = "/Volumes/GoogleDrive"
google_drive = "J:"
ch = "\\"
json_mapping_path = google_drive+("\My Drive\D-Data\C-Research\C-City-Data\B-Shenzhen\C-Subway\\3-Lines\station-locations.json".replace("\\",ch))
mapping = json.load(open(json_mapping_path))

#%%
from geopy.distance import vincenty
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
class Subway():
    def distanceAdjStation(self):
        result = []
        subway_staiton_file ="data/1-stations/subway_lines.geojson"
        fp =  open(subway_staiton_file)
        data = json.load(fp)
        for feature in data['features']:
            start_pt = feature['geometry']['coordinates'][0]
            end_pt = feature['geometry']['coordinates'][1]
            dis = abs(vincenty(start_pt, end_pt).km)
            if dis > 0:
                result.append(dis)
        print(result)
        fp.close()
#        plt.plot(result)
        r = pd.Series(result)
        ax = r.plot(kind='density')
        ax.set_ylabel("Density",fontsize=20)
        ax.legend(loc=0,fontsize=13)#,labels=["Light Rain","No Rain","Heavy Rain"])
        ax.set_xlabel("Distance (km)",fontsize=20)
        ax.set_ylim((0,1))
#        ax.set_xlim(0,23)
        plt.xticks(fontsize=15,rotation=90)
        plt.yticks(fontsize=20)
        print(r.mean())
        return(result)
#%% distance of subway stations
subway = Subway()
r = subway.distanceAdjStation()

#%% simulate circle
import os
import math
import json
def simulateCircle(centerLat,centerLon,N):
    circlePoints = []; radius = 580
    for k in xrange(N):
        angle = math.pi * 2* k/N; dx = radius * math.cos(angle); dy = radius*math.sin(angle);
        lat = centerLat + (180/math.pi)*(dy/6378137); lon = centerLon + (180/math.pi)*(dx/6378137)/math.cos(centerLat*math.pi/180);
        circlePoints.append([lon,lat])
    return(circlePoints)
def generateStationCoverage():
    year = "2017"
    google_drive = "J:\\"
    delay_origin_path = os.path.join(google_drive,"My Drive\W-WorkingOn\\1-coding\\1-research-projects\\0-Fine-Travel-Sigmetric2018\\5-Travel-Time-Subway\data\\1-processing\\0-delay\\1-year-origin.json".replace("\\",ch))
    delay_destination_path = os.path.join(google_drive,"My Drive\W-WorkingOn\\1-coding\\1-research-projects\\0-Fine-Travel-Sigmetric2018\\5-Travel-Time-Subway\data\\1-processing\\0-delay\\1-year-destination.json".replace("\\",ch))
    origin_json = json.load(open(delay_origin_path))[year]; destination_json = json.load(open(delay_destination_path))[year]
    json_mapping_path =  os.path.join(google_drive,"My Drive\D-Data\C-Research\C-City-Data\B-Shenzhen\C-Subway\\3-Lines\station-locations.json".replace("\\",ch))
    result = {}; out_edge = []; id_name = {}; name_id = {}
    ii = 0
    for k,v in mapping.items():
        if k in destination_json:
            circle_polygon = simulateCircle(v[0],v[1],20)
            out_edge.append({"geo_name":k,"geo_id":str(ii),"geo_array":circle_polygon, "center":v})
            id_name[str(ii)] = k
            name_id[k] = str(ii)
            ii += 1
    result['out_edge'] = out_edge; result['id_name'] = id_name; result['name_id'] = name_id
    json.dump(result,open("data/station-circle-polygon.json",'w'))
generateStationCoverage()