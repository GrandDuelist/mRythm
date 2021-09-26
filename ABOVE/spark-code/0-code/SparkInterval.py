# -*- coding: utf-8 -*-
from Interval import *
from Assistant import *
from TravelTimeInterval import *
class SparkInterval():
    def init(self):
        self.interval = TravelTimeInterval()
        self.taxi = self.interval

    def rowDataToRecords(self,input_data):
        '''
        :param input_data: row data
        :return: [(Record),(Record)]
        '''
        subway_records = input_data.flatMap(self.interval.parseOneSubwayRow).filter(lambda x: x is not None)#.map(lambda record: print(record.station_id))
        return(subway_records)


    def recordToSortedUserRecords(self,subway_records):
        '''
        :param subway_records:  [record,record,...]
        :return: [(user_id,[record1,record2,...]),(...)]
        '''
        user_record_list = subway_records.map(lambda x: (x.user_id,x)).groupByKey()
        sorted_user_records = user_record_list.mapValues(list).filter(lambda (k,v): len(v) > 1).map(lambda (k,v): (k, sorted(v, key=lambda record: record.time)))
        return(sorted_user_records)

    def buildRecordList(self,input_data):
        '''
        build the record list for each user and sort it by the time
        :return:
        '''
        self.subway_record_list = input_data.flatMap(self.taxi.parseOneTaxiRowToRecords).filter(lambda x: x is not None)
    
    def buildGPSRecordList(self,input_data):
        self.record_list = input_data.map(self.taxi.parseOneTaxiGPSToRecord).filter(lambda x: x is not None)

    def buildTripListFromGPS(self):
        record_group_user = self.record_list.groupBy(lambda record: record.plate)
        sorted_record_group_user = record_group_user.mapValues(list).map(lambda (k,v): (k, sorted(v, key=lambda record: record.time)))
        print("sort_record_user")
        self.user_trip_pair = sorted_record_group_user.map(self.taxi.parseRecordTotrip).filter(lambda (p,trip_list): len(trip_list) > 0)
        print(self.user_trip_pair.count())
    
    def buildNoUserTripList(self,input_data):
        self.trip_list = input_data.map(self.interval.parseOneTaxiRowToTrip).filter(lambda x: x is not None)

    def buildNoUserTripListOD(self,input_data):
        self.trip_list = input_data.map(self.interval.parseOneTaxiRowToTripOD).filter(lambda x: x is not None)
        print(self.trip_list.count())

    def buildNoUserTripListFromGPS(self):
       self.trip_list =  self.user_trip_pair.flatMap(self.taxi.userTripToNoUserTripList)
       self.trip_list = self.trip_list.map(self.taxi.mapTripGPSToGrid)
    
    

    def buildAverageByKey(self,key_values):
        key_value_count = key_values.combineByKey(
            lambda v: (v,1),
            lambda x,v: (x[0]+v,x[1]+1),
            lambda x,y: (x[0]+y[0],x[1]+y[1])
        )
        average_time = key_value_count.map(
            lambda (k,v): (k,float(v[0])/float(v[1]))
        )
        return(average_time)
    
