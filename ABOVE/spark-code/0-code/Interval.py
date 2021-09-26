# -*- coding: utf-8 -*-
from Assistant import *
from TripEntity import *
from Route import *
from datetime import timedelta
from TripHandle import *
class Interval():
    def __init__(self):
        self.file_path = None
        self.dir_path = None
        self.record_list = None
        self.all_trips = None
        self.time_matrix = None
        self.in_vehicle_time = None
        self.start_station = None
        self.end_station = None
        self.trip_handle = TripHandle()
        self.time_assist = TimeAssistant()

        self.minx,self.maxx,self.miny,self.maxy =  113.7463515, 114.6237079, 22.4415225, 22.8644043
        self.grid_shape = (50,50)
        self.initGrid()


    def init(self):
        self.file_path = None
        self.dir_path = None
        self.record_list = None
        self.all_trips = None
        self.time_matrix = None
        self.in_vehicle_time = None
        self.start_station = None
        self.end_station = None
        self.trip_handle = TripHandle()
        self.time_assist = TimeAssistant()
        self.minx,self.maxx,self.miny,self.maxy =  113.7463515, 114.6237079, 22.4415225, 22.8644043
        self.grid_shape = (50,50)
        self.initGrid()

    def initGrid(self):
        self.step = ((self.maxx - self.minx)/self.grid_shape[0],(self.maxy-self.miny)/self.grid_shape[1])


    def parseOneTaxiRowToRecords(self,row):
        try:
            attrs = row.split(",")
            start_time = self.time_assist.parseTimeWithouDate(attrs[2])
            end_time = self.time_assist.parseTimeWithouDate(attrs[6])
            start_time = self.time_assist.parseDate(attrs[1]) + timedelta(hours=start_time.hour,minutes=start_time.minute,seconds=start_time.second)
            end_time = self.time_assist.parseDate(attrs[5]) + timedelta(hours=end_time.hour,minutes=end_time.minute,seconds=end_time.second)
            start_record = TaxiODRecord(plate=attrs[0], time = start_time,lon = float(attrs[3]), lat = float(attrs[4]))
            end_record = TaxiODRecord(plate=attrs[0], time= end_time, lon = float(attrs[7]), lat = float(attrs[8]))
            return([start_record, end_record])
        except Exception as e:
            print(e)
            return([None,None])
    def parseOneTaxiODRowToRecords(self,row):
        try:
            attrs = row.split(",")
            start_time = self.time_assist.parseTime(attrs[2].replace("T"," ").replace(".000Z",""))
            end_time = self.time_assist.parseTime(attrs[6].replace("T"," ").replace(".000Z",""))
            start_record =TaxiODRecord(plate=attrs[0],time=start_time,lon=float(attrs[4]),lat=float(attrs[5]))
            end_record = TaxiODRecord(plate=attrs[0],time=end_time,lon=float(attrs[9]),lat=float(attrs[10]))
            return([start_record,end_record])
        except Exception as e:
            print(e)
            return([None,None])

    def parseOneTaxiRowToTrip(self,row):
        [start_record, end_record] = self.parseOneTaxiRowToRecords(row)
        if start_record is None or end_record is None:
            return(None)
        else:
            trip = Trip(start=start_record, end=end_record, start_time=start_record.time, end_time= end_record.time)
            travel_time  = trip.computeTripTimeToSeconds()
            self.mapTripGPSToGrid(trip)
            if travel_time == 0 :
                return(None)
            else:
                return(trip)
    def parseOneTaxiRowToTripOD(self,row):
        [start_record, end_record] = self.parseOneTaxiODRowToRecords(row)
        if start_record is None or end_record is None:
            return(None)
        else:
            trip = Trip(start=start_record, end=end_record, start_time=start_record.time, end_time= end_record.time)
            travel_time  = trip.computeTripTimeToSeconds()
            self.mapTripGPSToGrid(trip)
            if travel_time == 0 :
                return(None)
            else:
                return(trip)
    def mapRecordGPSToGrid(self,gps):
        gridx = (gps.lon - self.minx) / self.step[0]
        gridy = (gps.lat - self.miny) / self.step[1]
        return(str(int(gridx))+"-"+str(int(gridy)))
    
    def mapTripGPSToGrid(self,trip):
        start_grid = self.mapRecordGPSToGrid(trip.start)
        end_grid = self.mapRecordGPSToGrid(trip.end)
        trip.start.grid = start_grid; trip.end.grid = end_grid
        return(trip)
    

    """粤B000H6,红的,深圳市迅达汽车运输有限公司,113.910484,22.537018,2014-04-01T15:43:20.000Z,1086907,58,0,0,,,0,蓝色"""
    def parseOneTaxiGPSToRecord(self,row):
        try:
            attrs = row.split(",")
            time = self.time_assist.parseTime(attrs[5].replace("T"," ").replace(".000Z",""))
            lon = float(attrs[3]); lat = float(attrs[4])
            record = TaxiRecord(plate=attrs[0],time=time,lat=lat,lon=lon)
            return(record)
        except Exception as e:
            print(e)
            return(None)

    def parseRecordTotrip(self,record_list):
        return(self.trip_handle.parseRecordTotrip(record_list))

    def userTripToNoUserTripList(self,user_trip_list):
        (user,trip_list) = user_trip_list
        all_trips = []
        for one_trip in trip_list:
            one_trip.user_id = user
            all_trips.append(one_trip)
        return(all_trips)
    

# a="粤B141ZD,2017-05-31,14:26:05,114.059082,22.528032,2017-05-31,14:26:05,114.059082,22.528032,0,红的,260,268,00小时02分40秒,1400,0,0000000000000000,0,0,0,00小时00分00秒,0,0"
# test = Interval()
# result = test.parseOneTaxiRowToRecords(a)
# print(result)
