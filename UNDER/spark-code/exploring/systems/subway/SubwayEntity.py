 #coding=utf-8
import json
class SubwayRecord():
    def __init__(self,lon=None,lat=None,user_id=None,time=None,station_id=None,station_name=None,route_name=None,train_id=None,in_station=None):
        self.lon = lon
        self.lat = lat
        self.time = time
        self.station_id = station_id
        self.station_name = station_name
        self.route_name = route_name
        self.in_station = in_station
        self.train_id = train_id
        self.user_id = user_id
        self.district = None
    def setLocation(self,lon,lat):
        self.lon = lon
        self.lat = lat
    def setDistrictByStationName(self,subway):
        subway_station_name = self.station_name + "地铁站"
        #print subway_station_name
        self.district = subway.mapStationNameToDistrict(subway_station_name)


    def toJsonString(self):
        to_json  = {'lon': self.lon, 'lat': self.lat, 'time': self.time, 'station_id': self.station_id,
                      'station_name': self.station_name, 'route_name':self.station_name,
                      'in_station': self.in_station, 'train_id': self.train_id,  'user_id': self.user_id }
        to_json = json.dumps(to_json)
        return to_json
