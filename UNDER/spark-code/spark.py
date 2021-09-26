# -*- coding: utf-8 -*-
from __future__ import print_function
from pyspark import SparkConf
from pyspark import SparkContext

from pyspark.files import SparkFiles
from Spark import *
from TravelTimeSparkInterval import *
from hdfs import Config
import collections

class Spark():
    def __init__(self):
        self.sc = None
        self.file_path = None
        self.input_data = None
    def connect(self):
        self.connectToSchool()

    def setLocalInputFile(self,inputFile):
        self.input_data = self.sc.parallelize(inputFile)

    def connectToSchool(self):
        # SparkContext.setSystemProperty("mapreduce.input.fileinputformat.input.dir.recursive",True)
        conf=SparkConf()
        conf.setMaster("spark://namenode:7077")
        conf.setAppName("off-peak")
        conf.set("spark.executor.memory", "54g")
        conf.set("spark.executor.cores", "8")
        conf.set("spark.scheduler.mode", "FAIR")
        # conf.set("mapreduce.input.fileinputformat.input.dir.recursive", True)
        sc = SparkContext(conf=conf)
        self.sc =sc

    def setInputData(self,input_data):
        self.input_data = input_data
    def setLocalFilePath(self,file_path):
        self.file_path = file_path
        self.setLocalInputFile(open(self.file_path))
    def setHDFSFilePath(self,file_path):
        self.file_path = file_path
        self.setInputData(self.sc.textFile(self.file_path,use_unicode=False).cache())




class TravelTimeSpark(Spark):
    def __init__(self):
        self.connect()
        self.addExternalFiles()
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
        self.sc.addPyFile("exploring/systems/subway/Assistant.py")
        self.sc.addPyFile("exploring/systems/subway/Interval.py")
        self.sc.addPyFile("exploring/systems/subway/Spark.py")
        self.sc.addPyFile("exploring/systems/subway/SparkInterval.py")
        self.sc.addPyFile("exploring/systems/subway/TaxiEntity.py")
        self.sc.addPyFile("exploring/systems/subway/TravelTimeSpark.py")
        self.sc.addPyFile("exploring/systems/subway/TravelTimeInterval.py")
        self.sc.addPyFile("exploring/systems/subway/TravelTimeSparkInterval.py")
        self.sc.addPyFile("exploring/systems/subway/TravelTimeSparkMain.py")
        self.sc.addPyFile("exploring/systems/subway/TripEntity.py")
        self.sc.addPyFile("exploring/systems/subway/Route.py")
        self.sc.addPyFile("exploring/systems/subway/RouteEntity.py")

    def addExternalFiles(self):
        self.sc.addFile("data/shenzhen_subway_station_line.csv")
        self.subway_station_line = SparkFiles.get("shenzhen_subway_station_line.csv")

    def dailyNumberOfTrips(self):
        records = self.spark_interval.rowDataToRecords(input_data=self.input_data)
        return(records.count())

    def dailyNumberOfStation(self):
        records = self.spark_interval.rowDataToRecords(input_data=self.input_data)

        stations = records.map(lambda x: (x.station_name,x)).groupByKey()
        return(stations.count())

    def countTapPasengerInStation(self,input_dir,output_dir,local_output_dir,file_prefix):
        client = Config().get_client('dev')
        days = client.list(input_dir)
        results = {}
        m = {}
        for day in days:
            try:
                date_info = day
                if file_prefix in day:
                    date_info = str(day).lstrip(file_prefix)
                month = date_info[:6]
                if month in m:
                    print('Missed: time %s was processed' % month)
                    continue
                file_path = input_dir + day
                self.setHDFSFilePath(file_path)
                self.spark_interval.buildRecordList(self.input_data)
                self.spark_interval.buildTripList()
                self.spark_interval.buildNoUserTripList()
                self.spark_interval.filterTripListByStartEndStation()
                trip_time = self.spark_interval.tripStartTimeVsTravelTime()
                average_trip_time = self.spark_interval.buildAverageByKey(trip_time)
                average_trip_time.saveAsTextFile(output_dir+'average-daily-travel-time/Luohu-Shenzhenbei/'+month)
                one_result = average_trip_time.collect()
                print('INFO: Processing time %s' % month)
                results[month] =one_result
                m[month] = True
            except Exception as e:
                print("ERROR: file %s is damaged" % file_path)
                print(e)
        json.dump(results,open(local_output_dir+"daily-travel-time-month-Luohu-Shenzhenbei.json",'w'))

    def travelTimeBetweenTwoStation(self,start_station,end_station,input_dir,output_dir,local_output_dir,file_prefix):
        client = Config().get_client('dev')
        days = client.list(input_dir)
        results = {}
        m = {}
        for day in days:
            try:
                date_info = day
                if file_prefix in day:
                    date_info = str(day).lstrip(file_prefix)
                month = date_info[:6]
                if month in m:
                    print('Missed: time %s was processed' % month)
                    continue
                file_path = input_dir + day
                self.setHDFSFilePath(file_path)
                self.spark_interval.buildRecordList(self.input_data)
                self.spark_interval.buildTripList()
                self.spark_interval.buildNoUserTripList()
                self.spark_interval.filterTripListByStartEndStation(start_station,end_station)
                trip_time = self.spark_interval.tripStartTimeVsTravelTime()
                average_trip_time = self.spark_interval.buildAverageByKey(trip_time)
                average_trip_time.saveAsTextFile(output_dir+'average-daily-travel-time/Luohu-Shenzhenbeizhan/'+month)
                one_result = average_trip_time.collect()
                print('INFO: Processing time %s' % month)
                results[month] =one_result
                m[month] = True
            except Exception as e:
                print("ERROR: file %s is damaged" % file_path)
                print(e)
        json.dump(results,open(local_output_dir+"daily-travel-time-month-Luohu-Shenzhenbei.json",'w'))


    def startTimeVsNumberOfTrip(self,input_dir,output_dir,local_output_dir,file_prefix):
        client = Config().get_client('dev')
        days = client.list(input_dir)
        m = {}
        results = {}
        for day in days:
            try:
                date_info = day
                if file_prefix in day:
                    date_info = str(day).lstrip(file_prefix)
                month = date_info[:6]
                if month in m:
                    print('Missed: time %s was processed' % month)
                    continue
                file_path = input_dir + day
                self.setHDFSFilePath(file_path)
                count = self.dailyNumberOfTrips()
                sc_count = self.sc.parallelize([count])
                sc_count.saveAsTextFile(output_dir+'average-daily-number-of-trips/'+month)
                print('INFO: Processing time %s' % month)
                results[month] =count
                m[month] = True
            except Exception as e:
                print("ERROR: file %s is damaged" % file_path)
                print(e)
        json.dump(results,open(local_output_dir+"trip-number-month.json",'w'))

    def numberOfStations(self,input_dir,output_dir,local_output_dir,file_prefix):
        client = Config().get_client('dev')
        days = client.list(input_dir)
        m = collections.defaultdict(list)
        results = {}
        for day in days:
            try:
                date_info = day
                if file_prefix in day:
                    date_info = str(day).lstrip(file_prefix)
                month = date_info[:6]
                if month in m:
                    print('Missed: time %s was processed' % month)
                    continue
                file_path = input_dir + day
                # file_path = self.public_sample_subway_separate
                self.setHDFSFilePath(file_path)
                count = self.dailyNumberOfStation()
                sc_count = self.sc.parallelize([count])
                sc_count.saveAsTextFile(output_dir+'daily-number-of-stations/'+month)
                print('INFO: Processing time %s' % month)
                results[month] =count
                m[month] = True
            except Exception as e:
                print("ERROR: file %s is damaged" % file_path)
                print(e)
        json.dump(results,open(local_output_dir+"daily-number-of-stations.json",'w'))

    def mapRecordToODDelayRatio(self,file_path=None,month=""):
        if file_path is None:
            # file_path = self.public_sample
            day = '20130705'
            file_path = "/zf72/transportation_data/subway/input/%s" % day
            month = day[:6]
        self.setHDFSFilePath(file_path)
        self.spark_interval.sc = self.sc
        self.spark_interval.input_data = self.input_data
        self.spark_interval.buildPolygonMapping("/zf72/edge/station_with_tran_region.txt")
        self.spark_interval.mapRecordsToPolygon()
        self.spark_interval.buildTripList()
        self.spark_interval.buildNoUserTripList()
        delay_ratio = self.spark_interval.buildODStartVsTravelTime()
        delay_ratio.saveAsTextFile('/zf72/spark-results/subway/delay-ratio/%s' % month)
    def mapRecordToODDelayRatioStationToStation(self,file_path=None,month=""):
        if file_path is None:
            # file_path = self.public_sample
            day = '20130705'
            file_path = "/zf72/transportation_data/subway/input/%s" % day
            month = day[:6]
        self.setHDFSFilePath(file_path)
        self.spark_interval.sc = self.sc
        self.spark_interval.input_data = self.input_data
        self.spark_interval.buildPolygonMapping("/zf72/edge/station_with_tran_region.txt")
        self.spark_interval.mapRecordsToPolygon()
        self.spark_interval.buildTripList()
        self.spark_interval.buildNoUserTripList()
        delay_ratio = self.spark_interval.buildODStartVsTravelTimeStationToStation()
        delay_ratio =delay_ratio.map(lambda x: ','.join([str(x[0]),str(x[1]),str(x[2]),str(x[3])]))
        delay_ratio.saveAsTextFile('/zf72/spark-results/subway/delay-ratio-station/%s' % month)

    def mapRecordToODDelayRatioMonthStationToStation(self,input_dir,file_prefix):
        client = Config().get_client('dev')
        days = client.list(input_dir)
        # m = collections.defaultdict(list)
        m = {}
        for day in days:
            try:
                date_info = day
                if file_prefix in day:
                    date_info = str(day).lstrip(file_prefix)
                month = date_info[:6]
                if month in m:
                    print('Missed: time %s was processed' % month)
                    continue
                file_path = input_dir + day
                self.mapRecordToODDelayRatioStationToStation(file_path,month)
                print('INFO: Processing time %s' % day)
                m[month] = True
            except Exception as e:
                print("ERROR: file %s is damaged" % file_path)
                print(e)

    def mapRecordToODDelayRatioMonth(self,input_dir,file_prefix):
        client = Config().get_client('dev')
        days = client.list(input_dir)
        # m = collections.defaultdict(list)
        m = {}
        for day in days:
            try:
                date_info = day
                if file_prefix in day:
                    date_info = str(day).lstrip(file_prefix)
                month = date_info[:6]
                if month in m:
                    print('Missed: time %s was processed' % month)
                    continue
                file_path = input_dir + day
                self.mapRecordToODDelayRatio(file_path,month)
                print('INFO: Processing time %s' % day)
                m[month] = True
            except Exception as e:
                print("ERROR: file %s is damaged" % file_path)
                print(e)

    def averageDelayRatioOneMonthStation(self,file_path=None):
        if file_path is None:
            file_path = self.delay_ratio_sample
        self.setHDFSFilePath(file_path)
        self.spark_interval.sc =self.sc
        self.spark_interval.input_data = self.input_data
        self.delay_ratio_one_day = self.spark_interval.averageDelayRatioOneDayByStation()
        return(self.delay_ratio_one_day)

    def averageDelayRatioOneMonth(self,file_path=None):
        if file_path is None:
            file_path = self.delay_ratio_sample
        self.setHDFSFilePath(file_path)
        self.spark_interval.sc =self.sc
        self.spark_interval.input_data = self.input_data
        self.delay_ratio_one_day = self.spark_interval.averageDelayRatioOneDay()
        return(self.delay_ratio_one_day)

    def averageDelayRatioMultiMonth(self,input_dir,file_prefix,local_output_dir):
        client = Config().get_client('dev',)
        days = client.list(input_dir)
        m = collections.defaultdict(list)
        results = {}
        for day in days:
            try:
                date_info = day
                if file_prefix in day:
                    date_info = str(day).lstrip(file_prefix)
                month = date_info[:6]
                if month in m:
                    print('Missed: time %s was processed' % month)
                    continue
                file_path = input_dir + day
                delay_ratio = self.averageDelayRatioOneMonth(file_path)
                delay_ratio.saveAsTextFile('/zf72/spark-results/subway/delay-ratio-month/%s' % month)
                count = delay_ratio.map(lambda (k,v): v).count()
                total = delay_ratio.map(lambda (k,v): v).sum()
                average = float(total)/float(count)
                results[month] = average
                print('INFO: Processing time %s' % month)
            except Exception as e:
                print("ERROR: file %s is damaged" % file_path)
                print(e)
        json.dump(results,open(local_output_dir+"monthly-average-delay-ratio.json",'w'))

    def averageDelayRatioMultiMonthStation(self,input_dir,file_prefix,local_output_dir):
        client = Config().get_client('dev',)
        days = client.list(input_dir)
        m = collections.defaultdict(list)
        results = {}
        for day in days:
            try:
                date_info = day
                if file_prefix in day:
                    date_info = str(day).lstrip(file_prefix)
                month = date_info[:6]
                if month in m:
                    print('Missed: time %s was processed' % month)
                    continue
                file_path = input_dir + day
                delay_ratio = self.averageDelayRatioOneMonthStation(file_path)

                delay_ratio.saveAsTextFile('/zf72/spark-results/subway/average-station-delay-ratio-month/%s' % month)
                count = delay_ratio.map(lambda (k,v): v).count()
                total = delay_ratio.map(lambda (k,v): v).sum()
                average = float(total)/float(count)
                results[month] = average
                print('INFO: Processing time %s' % month)
            except Exception as e:
                print("ERROR: file %s is damaged" % file_path)
                print(e)
        json.dump(results,open(local_output_dir+"station-monthly-average-delay-ratio.json",'w'))

    def averageDelayRatioByHours(self,file_path=None):
        if file_path is None:
            file_path = self.delay_ratio_sample
        self.setHDFSFilePath(file_path)
        self.spark_interval.sc=self.sc
        self.spark_interval.input_data = self.input_data
        self.delay_ratio_hours = self.spark_interval.averageDelayRatioByHours()
        return(self.delay_ratio_hours)


    def averageDelayRatioByHoursMultiMonth(self,input_dir=None,local_output_dir=None,file_prefix=None):
        client = Config().get_client('dev',)
        days = client.list(input_dir)
        m = collections.defaultdict(list)
        results = {}
        for day in days:
            try:
                date_info = day
                if file_prefix in day:
                    date_info = str(day).lstrip(file_prefix)
                month = date_info[:6]
                if month in m:
                    print('Missed: time %s was processed' % month)
                    continue
                file_path = input_dir + day
                delay_ratio = self.averageDelayRatioByHours(file_path)
                delay_ratio.saveAsTextFile('/zf72/spark-results/subway/delay-ratio-hours-multi-month/%s' % month)
                # results[month] = delay_ratio.collect()
                m[month] = True
                print('INFO: Processing time %s' % month)
            except Exception as e:
                print("ERROR: file %s is damaged" % file_path)
                print(e)
        json.dump(results,open(local_output_dir+"hours-average-delay-ratio-multi-month.json",'w'))

    def averageDelayRatioSingleUserOneMonth(self,file_path=None,month=""):
        if file_path is None:
            file_path = self.public_sample
        self.setHDFSFilePath(file_path)
        self.spark_interval.sc = self.sc
        self.spark_interval.input_data = self.input_data
        self.spark_interval.buildPolygonMapping("/zf72/edge/station_with_tran_region.txt")
        self.spark_interval.mapRecordsToPolygon()
        self.spark_interval.buildTripList()
        self.spark_interval.buildNoUserTripList()
        self.spark_interval.tripListFilterByUser(user_id = '')
        delay_ratio = self.spark_interval.buildODStartVsTravelTime()
        return(delay_ratio)

    def averageDelayRationSingleUserMultiMonth(self,input_dir,local_output_dir,file_prefix):
        client = Config().get_client('dev',)
        days = client.list(input_dir)
        m = {}
        results = {}
        for day in days:
            try:
                date_info = day
                if file_prefix in day:
                    date_info = str(day).lstrip(file_prefix)
                month = date_info[:6]
                if month in m:
                    print('Missed: time %s was processed' % month)
                    continue
                file_path = input_dir + day
                delay_ratio = self.averageDelayRatioSingleUserOneMonth(input_dir)
                delay_ratio.saveAsTextFile('/zf72/spark-results/subway/delay-ratio-one-user/%s' % month)
                count = delay_ratio.map(lambda (k,v): v).count()
                total = delay_ratio.map(lambda (k,v): v).sum()
                average = float(total)/float(count)
                results[month] = average
                m[month] = True
                print('INFO: Processing time %s' % day)
            except Exception as e:
                print("ERROR: file %s is damaged" % file_path)
                print(e)
        json.dump(results,open(local_output_dir+"monthly-average-delay-ratio-one-user.json",'w'))

    def passengerDensityInRegions(self,file_path=None,month=""):
        if file_path is None:
            # file_path = self.public_sample
            file_path = self.subway_one_day
        self.setHDFSFilePath(file_path)
        self.spark_interval.sc = self.sc
        self.spark_interval.input_data = self.input_data
        self.spark_interval.buildPolygonMapping("/zf72/edge/station_with_tran_region.txt")
        self.spark_interval.mapRecordsToPolygon()
        self.spark_interval.buildTripList()
        self.spark_interval.buildNoUserTripList()
        passenger_density = self.spark_interval.tapInDensityPerRegionPerTimeUnit()

        passenger_density.saveAsTextFile('/zf72/spark-results/subway/passenger-region-density/%s' % month)
        return(passenger_density)

    def passengerDensityInStations(self,file_path=None,month=""):
        if file_path is None:
            # file_path = self.public_sample
            file_path = self.subway_one_day
        self.setHDFSFilePath(file_path)
        self.spark_interval.sc = self.sc
        self.spark_interval.input_data = self.input_data
        self.spark_interval.buildPolygonMapping("/zf72/edge/station_with_tran_region.txt")
        self.spark_interval.mapRecordsToPolygon()
        self.spark_interval.buildTripList()
        self.spark_interval.buildNoUserTripList()
        passenger_density = self.spark_interval.tapInDensityPerStationPerTimeUnit()
        passenger_density = passenger_density.map(lambda x: ','.join([str(x[0]),str(x[1]),str(x[2])]))
        passenger_density.saveAsTextFile('/zf72/spark-results/subway/passenger-station-density/%s' % month)
        return(passenger_density)


    def passengerDensityInRegionsMultiMonths(self,input_dir,local_output_dir,file_prefix):
        client = Config().get_client('dev',)
        days = client.list(input_dir)
        m = {}
        results = {}
        for day in days:
            try:
                date_info = day
                if file_prefix in day:
                    date_info = str(day).lstrip(file_prefix)
                month = date_info[:6]
                if month in m:
                    print('Missed: time %s was processed' % month)
                    continue
                file_path = input_dir + day
                delay_ratio = self.passengerDensityInRegions(file_path,month)
                # delay_ratio.saveAsTextFile('/zf72/spark-results/subway/delay-ratio-one-user/%s' % month)
                count = delay_ratio.map(lambda x: x[2]).count()
                total = delay_ratio.map(lambda x: x[2]).sum()
                average = float(total)/float(count)
                results[month] = average
                m[month] = True
                print('INFO: Processing time %s' % day)
            except Exception as e:
                print("ERROR: file %s is damaged" % file_path)
                print(e)
        json.dump(results,open(local_output_dir+"monthly-average-delay-passenger_density-region.json",'w'))


    def passengerDensityInStationsMultiMonths(self,input_dir,local_output_dir,file_prefix):
        client = Config().get_client('dev',)
        days = client.list(input_dir)
        m = {}
        results = {}
        for day in days:
            try:
                date_info = day
                if file_prefix in day:
                    date_info = str(day).lstrip(file_prefix)
                month = date_info[:6]
                if month in m:
                    print('Missed: time %s was processed' % month)
                    continue
                file_path = input_dir + day
                delay_ratio = self.passengerDensityInStations(file_path,month)
                # count = delay_ratio.map(lambda x: float(x[2])).count()
                # total = delay_ratio.map(lambda x: float(x[2])).sum()
                # average = float(total)/float(count)
                # results[month] = average
                m[month] = True
                print('INFO: Processing time %s' % day)
            except Exception as e:
                print("ERROR: file %s is damaged" % file_path)
                print(e)
        #json.dump(results,open(local_output_dir+"monthly-average-delay-passenger-density-station.json",'w'))


    def passengerDensityDelayRatioInStationsMultiMonths(self,density_input_dir,delay_input_dir,local_output_dir,file_prefix):
        client = Config().get_client('dev',)
        days = client.list(density_input_dir)
        m = {}
        results = {}
        for day in days:
            try:
                date_info = day
                if file_prefix in day:
                    date_info = str(day).lstrip(file_prefix)
                month = date_info[:6]
                if month in m:
                    print('Missed: time %s was processed' % month)
                    continue
                density_file_path = density_input_dir + day
                delay_file_path = delay_input_dir+day
                delay_ratio = self.passengerDensityDelayRatioInStations(density_file_path,delay_file_path,month)
                # delay_ratio.saveAsTextFile('/zf72/spark-results/subway/delay-ratio-one-user/%s' % month)
                m[month] = True
                print('INFO: Processing time %s' % day)
            except Exception as e:
                print("ERROR: file %s is damaged" % density_file_path)
                print(e)

    def passengerDensityDelayRatioInStations(self,file_path_density=None,file_path_delay_ratio=None,month=""):
        self.spark_interval.sc = self.sc
        self.density_data = self.sc.textFile(file_path_density,use_unicode=False).cache()
        self.delay_ratio = self.sc.textFile(file_path_delay_ratio,use_unicode=False).cache()
        density = self.density_data.map(self.spark_interval.subway.parseTuple).map(lambda x: ((x[0],x[1]),x))

        delay = self.delay_ratio.map(self.spark_interval.subway.parseTuple).map(lambda x: ((x[0],x[2]),x))
        combined = delay.join(density).map(lambda (k,v): (v[0][0],v[0][1],v[0][2],v[0][3],v[1][2]))
        combined = combined.map(lambda x: ','.join([str(v) for v in x]))
        combined.saveAsTextFile('/zf72/spark-results/subway/density-delay-ratio-in-station/%s' % month)
        return(combined)


    def filterDelayRatioByStartEndStationMultiMonth(start_station,end_station,input_dir,output_dir,file_prefix):
        pass


