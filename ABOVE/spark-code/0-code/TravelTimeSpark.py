 #coding=utf-8
from pyspark.files import SparkFiles
from Spark import *
from TravelTimeSparkInterval import *
from hdfs import Config
import collections

# -*- coding: utf-8 -*-
import json
class TravelTimeSpark(Spark):
    def __init__(self,sc=None):
        if sc is None:
            self.connectToSchool()
        else:
            self.connectToSpyder(sc)
        self.addPytdhonFiles()
        self.spark_interval = TravelTimeSparkInterval(sc=self.sc)
        self.interval = TravelTimeInterval()
        self.setTestFilePath()

    def setTestFilePath(self):
        self.public_sample = "/zf72/transportation_data/sample/input/SZT_sample_0601.txt"
        self.public_sample_old = "/zf72/transportation_data/sample/input/public_sample_subway.txt"
        self.public_sample_subway_separate = "/zf72/transportation_data/sample/input/public_sample_subway.txt"
        self.subway_one_day = "/zf72/transportation_data/subway/input/20130910"
        self.delay_ratio_sample= ""

    def addPytdhonFiles(self):
        #self.current_work_dir = r"E:\drive\W-WorkingOn\1-coding\1-research-projects\0-Fine-Travel-Sigmetric2018\2-Spark-Taxi\0-code\\"
        self.current_work_dir = ""
        self.sc.addPyFile(self.current_work_dir+"Assistant.py")
        self.sc.addPyFile(self.current_work_dir+"Interval.py")
        self.sc.addPyFile(self.current_work_dir+"Spark.py")
        self.sc.addPyFile(self.current_work_dir+"SparkInterval.py")
        self.sc.addPyFile(self.current_work_dir+"TaxiEntity.py")
        self.sc.addPyFile(self.current_work_dir+"TravelTimeSpark.py")
        self.sc.addPyFile(self.current_work_dir+"TravelTimeInterval.py")
        self.sc.addPyFile(self.current_work_dir+"TravelTimeSparkInterval.py")
        self.sc.addPyFile(self.current_work_dir+"TravelTimeSparkMain.py")
        self.sc.addPyFile(self.current_work_dir+"TripEntity.py")
        self.sc.addPyFile(self.current_work_dir+"Route.py")
        self.sc.addPyFile(self.current_work_dir+"RouteEntity.py")
        self.sc.addPyFile(self.current_work_dir+"TripHandle.py")



    def startTimeNumberOfTripMultiMonths(self,input_dir=None, file_prefix = None, output_dir= None,local_output_dir = None):
        client = Config().get_client('dev')
        days = client.list(input_dir)
        m = {}
        results = {}
        for day in days:
            try:
                date_info = day
                if file_prefix in day:
                    date_info = str(day).lstrip(file_prefix)
                month = date_info[:7]
                if month in m:
                    print('Missed: time %s was processed' % month)
                    continue
                file_path = input_dir + day
                count = self.startTimeVsNumberOfTripOneMonth(file_path=file_path,month=month,output_dir=output_dir+"start-time-vs-number-of-trips")
                print('INFO: Processing time %s' % month)
                results[month] = count
                m[month] = True
            except Exception as e:
                print("ERROR: file %s is damaged" % file_path)
                print(e)
        json.dump(results,open(local_output_dir+"start-time-vs-number-of-trip.json",'w'))


    def startTimeVsNumberOfTripOneMonth(self,file_path=None,month=None,output_dir = None):
        if file_path is None:
            # file_path = self.public_sample
            day = '2013-09-08'
            file_path = "/zf72/transportation_data/taxi-od/input/%s" % day
            month = day[:7]
        self.setHDFSFilePath(file_path)
        self.spark_interval.sc = self.sc
        self.spark_interval.input_data = self.input_data
        self.spark_interval.buildNoUserTripList(self.input_data)
        count = self.spark_interval.trip_list.count()
        hour_count = self.spark_interval.trip_list.map(lambda x: (x.timeSlot(t_hour=1),1)).reduceByKey(lambda a,b: a+b)
        hour_count = hour_count.map(lambda x: str(x[0])+","+str(x[1]))
        hour_count.saveAsTextFile((output_dir+'/%s') % month)
        return(count)

    def startTimeDelayRatio(self,file_path=None):
        if file_path is None:
            # file_path = self.public_sample
            day = '20130705'
            file_path = "/zf72/transportation_data/taxi-od/input/%s" % day
            month = day[:6]
        self.setHDFSFilePath(file_path)
        self.spark_interval.sc = self.sc
        self.spark_interval.input_data = self.input_data
        self.spark_interval.buildPolygonMapping("/zf72/edge/station_with_tran_region.txt")
        self.spark_interval.mapRecordsToPolygon()
        self.spark_interval.buildTripList()
        self.spark_interval.buildNoUserTripList()
        delay_ratio = self.spark_interval.buildODStartVsTravelTime()
        delay_ratio.saveAsTextFile('/zf72/spark-results/taxi-od//%s' % month)

    def startTimeVsNumberOfVehicleMultiMonthes(self,input_dir=None, file_prefix = None, output_dir= None,local_output_dir = None):
        client = Config().get_client('dev')
        days = client.list(input_dir)
        m = {}
        results = {}
        for day in days:
            try:
                date_info = day
                if file_prefix in day:
                    date_info = str(day).lstrip(file_prefix)
                month = date_info[:7]
                if month in m:
                    print('Missed: time %s was processed' % month)
                    continue
                file_path = input_dir + day
                count = self.startTimeVsNumberOfVehicleOneMonth(file_path=file_path,month=month,output_dir=output_dir+"start-time-vs-number-of-vehicles")
                print('INFO: Processing time %s' % month)
                results[month] = count
                m[month] = True
            except Exception as e:
                print("ERROR: file %s is damaged" % file_path)
                print(e)
        json.dump(results,open(local_output_dir+"start-time-vs-number-of-vehicle.json",'w'))

    def startTimeVsNumberOfVehicleOneMonth(self,file_path =None,month=None,output_dir = None):
        if file_path is None:
            # file_path = self.public_sample
            day = '2013-09-08'
            file_path = "/zf72/transportation_data/taxi-od/input/%s" % day
            month = day[:7]
        self.setHDFSFilePath(file_path)
        self.spark_interval.sc = self.sc
        self.spark_interval.input_data = self.input_data
        self.spark_interval.buildNoUserTripList(self.input_data)
        start_time_vehicles = self.spark_interval.trip_list.map(lambda x: ((x.timeSlot(t_hour=1),x.start.plate),1)).groupByKey().map(lambda k,v:k)
        #start_time_vehicles.map().reduceByKey(lambda a,b: a+b)
        count = start_time_vehicles.map(lambda k,v: (k[1],1)).groupByKey().count()
        hour_count = start_time_vehicles.map(lambda x: (x[0],1)).reduceByKey(lambda a,b: a+b)
        hour_count = hour_count.map(lambda x: str(x[0])+","+str(x[1]))
        hour_count.saveAsTextFile((output_dir+'/%s') % month)
        return(count)


    def odTimeDelayRatioMultiMonthes(self,input_dir=None, file_prefix = None, output_dir= None,local_output_dir = None):
        client = Config().get_client('dev')
        days = client.list(input_dir)
        m = {}
        for day in days:
            try:
                date_info = day
                if file_prefix in day:
                    date_info = str(day).lstrip(file_prefix)
                month = date_info[:7]
                if month in m:
                    print('Missed: time %s was processed' % month)
                    continue
                file_path = input_dir + day
                self.odTimeDelayRatioOneMonth(file_path=file_path,month=month,output_dir=output_dir+"delay-ratio-grid")
                m[month] = True
                print('INFO: Processing time %s' % month)
            except Exception as e:
                print("ERROR: file %s is damaged" % file_path)
                print(e)


    def odTimeDelayRatioOneMonth(self,file_path =None,month=None,output_dir = None):
        if file_path is None:
            # file_path = self.public_sample
            day = '2015-09-08'
            file_path = "/zf72/transportation_data/taxi-od/input/%s" % day
            month = day[:7]
        self.setHDFSFilePath(file_path)
        self.spark_interval.sc = self.sc
        self.spark_interval.input_data = self.input_data
        self.spark_interval.buildNoUserTripList(self.input_data)
        self.spark_interval.odTimeDelayRatio()
        # self.spark_interval.trip_list.saveAsTextFile((output_dir+'/%s') % month)
        output = self.spark_interval.grid_slot_delay.map(lambda x: ','.join([str(x[0][0]),str(x[0][1]),str(x[0][2]),str(x[1])]))
        output.saveAsTextFile((output_dir+'/%s') % month)


    def averageDelayRatioMultiMonth(self,input_dir=None,output_dir=None,local_output_dir=None,file_prefix=None):
        client = Config().get_client('dev')
        days = client.list(input_dir)
        m = {}
        results = {}
        for day in days:
            try:
                date_info = day
                if file_prefix in day:
                    date_info = str(day).lstrip(file_prefix)
                month = date_info[:7]
                if month in m:
                    print('Missed: time %s was processed' % month)
                    continue
                file_path = input_dir + day
                delay_ratio = self.averageDelayRatioOneMonth(file_path=file_path,month=month,output_dir=output_dir+"daily-average-delay-ratio-by-pairs")
                count = delay_ratio.map(lambda (k,v): v).count()
                total = delay_ratio.map(lambda (k,v): v).sum()
                average = float(total)/float(count)
                results[month] = average
                m[month] = True
                print('INFO: Processing time %s' % month)
            except Exception as e:
                print("ERROR: file %s is damaged" % file_path)
                print(e)



    def averageDelayRatioOneMonth(self,file_path=None,month = None,output_dir=None):
        if file_path is None:
            file_path = self.delay_ratio_sample
        self.setHDFSFilePath(file_path)
        self.spark_interval.sc =self.sc
        self.spark_interval.input_data = self.input_data
        self.delay_ratio_one_day = self.spark_interval.averageDelayRatioOneDayByODPairs()
        # self.delay_ratio_one_day = self.spark_interval.
        output = self.delay_ratio_one_day.map(lambda x: ','.join([str(x[0][0]),str(x[0][1]),str(x[0][2]),str(x[0][3]),str(x[1])]))
        output.saveAsTextFile((output_dir+'/%s') % month)
        
    def travelTimeBetweenLocations(self,input_dir,output_dir):
        client = Config().get_client('dev');days = client.list(input_dir);results = {};m = {}
        for day in days:
            try:
                date_info = day
                # if file_prefix in day:
                #     date_info = str(day).lstrip(file_prefix)
                month = date_info[:6]
                # if month in m:
                #     print('Missed: time %s was processed' % month)
                #     continue
                file_path = input_dir + day
                print(file_path)
                self.setHDFSFilePath(file_path)
                self.spark_interval.buildGPSRecordList(self.input_data)
                self.spark_interval.buildTripListFromGPS()
                self.spark_interval.buildNoUserTripListFromGPS()
                # self.spark_interval.filterTripListByStartEndStation(start_station,end_station)
                trip_time = self.spark_interval.tripGridStartTimeVsTravelTime()
                average_trip_time = self.spark_interval.buildStatisticsByKey(trip_time)
                average_trip_time.saveAsTextFile(output_dir+'0-travel-time/0-station-to-station/'+date_info)
                print('INFO: Processing time %s' % month)
                m[month] = True
            except Exception as e:
                print("ERROR: file %s is damaged" % file_path)
                print(e)

    def travelTimeBetweenLocationsOD(self,input_dir,output_dir):
        client = Config().get_client('dev');days = client.list(input_dir);results = {};m = {}
        for day in days:
            try:
                date_info = day
                month = date_info[:7]
                file_path = input_dir + day
                print(file_path)
                self.setHDFSFilePath(file_path)
                self.spark_interval.buildNoUserTripListOD(self.input_data)
                trip_time = self.spark_interval.tripGridStartTimeVsTravelTime()
                average_trip_time = self.spark_interval.buildStatisticsByKey(trip_time)
                average_trip_time.saveAsTextFile(output_dir+'0-travel-time-od/0-station-to-station/'+date_info)
                print('INFO: Processing time %s' % month)
                m[month] = True
            except Exception as e:
                print("ERROR: file %s is damaged" % file_path)
                print(e)
