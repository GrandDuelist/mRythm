from StatisticSparkInterval import *
from Spark import *
from StatisticInterval import *
from pyspark.files import SparkFiles
import collections
from hdfs import Config
class StatisticSpark(Spark):
    def __init__(self):
        self.connect()
        self.addExternalFiles()
        self.addPytdhonFiles()
        self.spark_interval = StatisticSparkInterval(sc=self.sc)
        self.interval= StatisticInterval()
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
        self.sc.addPyFile("exploring/systems/subway/SubwayEntity.py")
        self.sc.addPyFile("exploring/systems/subway/StatisticSpark.py")
        self.sc.addPyFile("exploring/systems/subway/StatisticInterval.py")
        self.sc.addPyFile("exploring/systems/subway/StatisticSparkInterval.py")
        self.sc.addPyFile("exploring/systems/subway/StatisticSparkMain.py")
        self.sc.addPyFile("exploring/systems/subway/TripEntity.py")
        self.sc.addPyFile("exploring/systems/subway/Route.py")
        self.sc.addPyFile("exploring/systems/subway/RouteEntity.py")

    def addExternalFiles(self):
        self.sc.addFile("data/shenzhen_subway_station_line.csv")

    def dailyNumberOfTrips(self,file_path=None):
        if file_path is None:
            # file_path = self.public_sample
            day = '20130705'
            file_path = "/zf72/transportation_data/subway/input/%s" % day
            month = day[:6]
        self.setHDFSFilePath(file_path)
        self.spark_interval.sc = self.sc
        self.spark_interval.input_data = self.input_data
        self.spark_interval.buildRecordList(self.input_data)
        self.spark_interval.buildTripList()
        self.spark_interval.buildNoUserTripList()
        count = self.spark_interval.trip_list.count()
        return(count)

    def dailyNumberOfTripMultiDays(self,input_dir,file_prefix,local_output_dir):
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
                count = self.dailyNumberOfTrips(file_path)
                results[month] = count
                print('INFO: Processing time %s' % month)
                m[month] = True
            except Exception as e:
                print("ERROR: file %s is damaged" % file_path)
                print(e)
        json.dump(results,open(local_output_dir+"daily-number-of-trip.json",'w'))

    def dailyNumberOfPassengers(self,file_path=None):
        if file_path is None:
            # file_path = self.public_sample
            day = '20130705'
            file_path = "/zf72/transportation_data/subway/input/%s" % day
            month = day[:6]
        self.setHDFSFilePath(file_path)
        self.spark_interval.sc = self.sc
        self.spark_interval.input_data = self.input_data
        record_list = self.spark_interval.rowDataToRecords(self.input_data)
        passenger_count = record_list.map(lambda x: (x.user_id,1)).reduceByKey(lambda x,y: x+y)
        count = passenger_count.count()
        return(count)

    def dailyNumberOfPassengersMultiDays(self,input_dir,file_prefix,local_output_dir):
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
                count = self.dailyNumberOfPassengers(file_path)
                results[month] = count
                print('INFO: Processing time %s' % month)
                m[month] = True
            except Exception as e:
                print("ERROR: file %s is damaged" % file_path)
                print(e)
        json.dump(results,open(local_output_dir+"daily-number-of-passenger.json",'w'))
