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

class BusRecord():
    def __init__(self,lon=None,lat=None,user_id=None,route_num=None,time=None,station_id=None,station_name=None,route_name=None,plate=None,in_station=None,stop_name=None,stop_time=None):
        self.lon = lon
        self.lat = lat
        self.time = time
        self.station_id = station_id
        self.station_name = station_name
        self.route_name = route_name
        self.in_station = in_station
        self.plate = plate
        self.user_id = user_id
        self.stop_name = stop_name
        self.stop_time = stop_time
        self.district = None
        self.trans_region = None
        self.route_num = route_num
        self.is_stopped =(not (self.stop_time == None))

    def setLocation(self,lon,lat):
        self.lon = lon
        self.lat = lat

    def toJsonString(self):
        to_json  = {'lon': self.lon, 'lat': self.lat, 'time': self.time, 'station_id': self.station_id,
                      'station_name': self.station_name, 'route_name':self.station_name,
                      'in_station': self.in_station, 'train_id': self.plate,  'user_id': self.user_id }
        to_json = json.dumps(to_json)
        return to_json


class TaxiRecord():
    def __init__(self,lon=None,lat=None,is_occupied=False,plate=None,time=None,station_id = None,stop_name=None, stop_time=None):
        self.lon = lon
        self.lat = lat
        self.is_occupied = is_occupied
        self.plate = plate
        self.time = time
        self.station_id = station_id
        self.trans_region = None
        self.district = None
        self.stop_name = stop_name
        self.stop_time = stop_time

    def computeTargetRegion(self,taxi):
        self.trans_region = taxi.findPointInTransRegion([self.lon,self.lat])

    def computeTargetDistrict(self,taxi):
        self.district = taxi.findPointInDistrict([self.lon,self.lat])

class PVRecord():
    def __init__(self,pv_id=None,lat=None,lon=None,time=None,station_id = None):
        self.pv_id = pv_id
        self.lat = lat
        self.lon = lon
        self.time = time
        self.station_id = station_id

    def computeTargetRegion(self,pv):
        self.trans_region = pv.findPointInTransRegion([self.lon,self.lat])

    def computeTargetDistrict(self,pv):
        self.district = pv.findPointInDistrict([self.lon,self.lat])



class SubwayRouteRecord():
    def __init__(self,line_num=None,station_name=None,transfer=None,translate=None,order = None,one_line = None):
        self.line_num = line_num
        self.station_name = station_name
        self.transfer =transfer
        self.translate = translate
        self.one_line = one_line
        self.order =order
