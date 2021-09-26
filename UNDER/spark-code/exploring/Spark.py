 #coding=utf-8
from __future__ import print_function
from pyspark import SparkConf
from pyspark import SparkContext
from Transportation import *
import json
from pyspark.files import SparkFiles
from operator import add
class Spark():
    def __init__(self):
        self.sc = None
        self.sorted_user_record_list = None
        self.file_path = None
        self.input_data = None
    def connect(self):
        # self.connectToSchool()
        self.connectToDesktop()
    def setLocalInputFile(self,inputFile):
        self.input_data = self.sc.parallelize(inputFile)

    def connectToSchool(self):
        conf=SparkConf()

        conf.setMaster("spark://namenode:7077")
        conf.setAppName("off-peak")
        conf.set("spark.executor.memory", "54g")
        conf.set("spark.executor.cores", "8")
        conf.set("spark.scheduler.mode", "FAIR")
        sc = SparkContext(conf=conf)
        sc.addPyFile("Module/Shapely-1.6b4.zip")
        sc.addFile("exploring/Spark.py")
        sc.addFile("exploring/Transportation.py")
        sc.addFile("exploring/Record.py")
        sc.addFile("exploring/TransRegions.py")
        sc.addFile("exploring/__init__.py")
        sc.addFile('exploring/SparkLocalMain.py')
        sc.addFile('exploring/Route.py')
        sc.addFile('exploring/Assistant.py')
        self.sc =sc

    def connectToDesktop(self):
        conf=SparkConf()
        # conf.setMaster("spark://localhost:7077")
        conf.setAppName("off-peak")
        conf.set("spark.executor.memory", "24g")
        conf.set("spark.executor.cores", "12")
        conf.set("spark.scheduler.mode", "FAIR")
        sc = SparkContext(conf=conf)
        # sc.clearFiles()
        # sc.addPyFile("Module/Shapely-1.6b4.zip")
        sc.addFile("exploring/Spark.py")
        sc.addFile("exploring/Transportation.py")
        sc.addFile("exploring/Record.py")
        sc.addFile("exploring/TransRegions.py")
        sc.addFile("exploring/__init__.py")
        sc.addFile('exploring/SparkLocalMain.py')
        sc.addFile('exploring/Route.py')
        sc.addFile('exploring/Assistant.py')
        self.sc = sc

    def setInputData(self,input_data):
        self.input_data = input_data
    def setLocalFilePath(self,file_path):
        self.file_path = file_path
        self.setLocalInputFile(open(self.file_path))
    def setHDFSFilePath(self,file_path):
        self.file_path = file_path
        self.setInputData(self.sc.textFile(self.file_path,use_unicode=False).cache())


class SubwaySpark(Spark):
    def __init__(self):
        self.subway = Subway()
        self.sorted_record_list = None
        self.user_trip_pair = None
        self.trip_average_time  = None
        self.in_vehicle_time = None
        self.connect()
        self.addExternalFiles()


    def addExternalFiles(self):
        self.sc.addFile("data/shenzhen_subway_station_line.csv")
        self.subway_station_line = SparkFiles.get("shenzhen_subway_station_line.csv")

    def buildRecordList(self):
        '''
        build the record list for each user and sort it by the time
        :return:
        '''
        one_file = self.input_data
        subway_record_list = one_file.map(self.subway.subwayOneRow).filter(lambda x: x is not None)#.map(lambda record: print(record.station_id))
        self.subway_record_list = subway_record_list
        user_record_list = subway_record_list.map(lambda x: (x.user_id,x)).groupByKey()
        self.sorted_user_record_list = user_record_list.mapValues(list).filter(lambda (k,v): len(v) > 1).map(lambda (k,v): (k, sorted(v, key=lambda record: record.time)))
    def buildStationNumber(self):
        one_file = self.input_data
        subway_record_list = one_file.map(self.subway.subwayOneRow).filter(lambda x: x is not None)
        stations = subway_record_list.map(lambda x: (SubwayRecord(x).station_name,1)).groupByKey()
        return(stations)
    def buildDistrictFilter(self,target_district):
        '''
        filter the record station by district
        :return:
        '''
        station_district_mapping = self.sc.textFile("/zf72/data/station_with_region.txt",use_unicode=False).cache()
        station_district_mapping = station_district_mapping.map(self.subway.stationDistrictMappingOneLine).filter(lambda (k,v): v==target_district)
        station_district_mapping = station_district_mapping.map(lambda (k,v): (v,k)).filter(lambda (k,v): '地铁站' in v)
        station_district_mapping = station_district_mapping.map(lambda (k,v): (v.replace('地铁站',''),k))
        self.station_district_mapping = station_district_mapping.reduceByKey(self.subway.removeDuplicateInMapping)

    def buildFilterRecordListByDistrict(self):
        one_file = self.input_data
        subway_record_list = one_file.map(self.subway.subwayOneRow).filter(lambda x: x is not None)#.map(lambda record: print(record.station_id))
        subway_record_list = subway_record_list.map(lambda record: (record.station_name.rstrip('站'),record))
        records_after_filter = subway_record_list.join(self.station_district_mapping).map(lambda (k,v): v[0])
        user_record_list = records_after_filter.map(lambda x: (x.user_id,x)).groupByKey()
        self.sorted_user_record_list = user_record_list.mapValues(list).filter(lambda (k,v): len(v) > 1).map(lambda (k,v): (k, sorted(v, key=lambda record: record.time)))

    def buildTripList(self):
        self.user_trip_pair = self.sorted_user_record_list.map(self.subway.sortedRecordsToTrip).filter(lambda (k,v): len(v)>0)

    def filterTripListByStartDistrict(self):
        user_trip_pair = self.user_trip_pair.flatMap(self.subway.user_trip_to_trip_user)
        user_trip_pair = user_trip_pair.map(lambda (user_id,one_trip): (one_trip.start.station_name.rstrip('站'),(one_trip,user_id)))
        filter_user_trip_pair = user_trip_pair.join(self.station_district_mapping)
        filter_user_trip_pair = filter_user_trip_pair.map(lambda (k,v): (v[0][1],v[0][0])).groupByKey().mapValues(list)
        self.user_trip_pair = filter_user_trip_pair


    def filterTripListByDestinationDistrict(self):
        user_trip_pair = self.user_trip_pair.flatMap(self.subway.user_trip_to_trip_user)
        user_trip_pair = user_trip_pair.map(lambda (user_id,one_trip): (one_trip.end.station_name.rstrip('站'),(one_trip,user_id)))
        filter_user_trip_pair = user_trip_pair.join(self.station_district_mapping)
        filter_user_trip_pair = filter_user_trip_pair.map(lambda (k,v): (v[0][1],v[0][0])).groupByKey().mapValues(list)
        self.user_trip_pair = filter_user_trip_pair

    def setStartEndStation(self,start_station=None,end_station=None):
        self.subway.start_station = start_station
        self.subway.end_station = end_station

    def buildNoUserTripList(self):
       self.trip_list =  self.user_trip_pair.flatMap(self.subway.userTripToNoUserTripList)

    def filterTripListByStartEndStation(self,start_station=None,end_station=None):
        self.setStartEndStation(start_station=start_station,end_station=end_station)
        self.filtered_trip_list = self.trip_list.filter(self.subway.filterByStartEndStation)

    def tripStartTimeVsTravelTime(self):
        self.trip_start_time = self.filtered_trip_list.map(lambda trip: (trip.timeSlot(t_min=60),trip.trip_time.total_seconds()))
        return(self.trip_start_time)

    def tripStartTimeVsRiddingTime(self,start_station,end_station):
        self.setStartEndStation(start_station,end_station)
        filter_user_trip_pair = self.user_trip_pair.flatMap(lambda (u,triplist): triplist).filter(self.subway.fitlerByStartEndStationByTrip)
        self.time_slot_ridding_time = filter_user_trip_pair.map(lambda (one_trip): (one_trip.timeSlot(t_min=60),one_trip.trip_time.total_seconds())).reduceByKey(self.subway.minimumByKey)


    def buildInVehicleTime(self):
        trip_time_pair = self.user_trip_pair.flatMap(self.subway.mapToStationTimeMapping)
        self.in_vehicle_time = trip_time_pair.reduceByKey(self.subway.minimumByKey)
        return(self.in_vehicle_time)
        #self.in_vehicle_time.saveAsTextFile('/zf72/transportation_data/result/in_vehicle_time/0601')

    def buildWaitingTimeInTwoStations(self,start_station=None,end_station=None):
        self.setStartEndStation(start_station,end_station)
        user_trips_in_vehicle_time = self.user_trip_pair.flatMap(self.subway.mapToODTrip).join(self.in_vehicle_time)
        filter_user_trips_in_vehicle_time = user_trips_in_vehicle_time.filter(self.subway.filterWaitingTimeByStartEndStation)

        self.timeslot_waiting_time = filter_user_trips_in_vehicle_time.map(self.subway.mapToTripWaitingTime).map(lambda (k,v1,v2): (k,v2))
        return(self.timeslot_waiting_time)



    def buildWaitingTime(self,output_file_name):
        #read in_vehicle_time mapping
        user_trips_in_vehicle_time = self.user_trip_pair.flatMap(self.subway.mapToODTrip).join(self.in_vehicle_time)
        timeslot_waiting_time = user_trips_in_vehicle_time.map(self.subway.mapToTripWaitingTime).map(lambda (k,v1,v2): (k,v2))
        self.timeslot_ave_waiting_time = timeslot_waiting_time.combineByKey(
            lambda v: (v,1),
            lambda x,v: (x[0]+v,x[1]+1),
            lambda x,y: (x[0]+y[0],x[1]+y[1])
        ).map(
            lambda (k,v): (k,float(v[0])/float(v[1]))
        )
        self.timeslot_ave_waiting_time.saveAsTextFile("/zf72/transportation_data/subway/output/waiting_time/"+output_file_name)

    def buildWaitingTimeStations(self,output_file_name):
        user_trips_in_vehicle_time = self.user_trip_pair.flatMap(self.subway.mapToODTrip).join(self.in_vehicle_time)
        # print(user_trips_in_vehicle_time.collect())

        od_timeslot_waiting_time = user_trips_in_vehicle_time.map(self.subway.mapToTripWaitingTimeByStation).map(lambda (ko,kt,v2): ((ko,kt),v2))
        # print(timeslot_waiting_time.collect())
        od_timeslot_waiting_time = od_timeslot_waiting_time.combineByKey(
            lambda v: (v,1),
            lambda x,v: (x[0]+v,x[1]+1),
            lambda x,y: (x[0]+y[0],x[1]+y[1])
        ).map(
            lambda (k,v): (k,float(v[0])/float(v[1]))
        )
        self.od_timeslot_ave_waiting_time = od_timeslot_waiting_time.map(lambda (k,v): ','.join([str(k[0]),str(k[1]),str(v)]))
        return(self.od_timeslot_ave_waiting_time)
        # self.timeslot_ave_waiting_time.saveAsTextFile("/zf72/transportation_data/subway/output/waiting_time/"+output_file_name)
        #

    def buildTripAveTimeMatrix(self):
        trip_time_pair = self.user_trip_pair.flatMap(self.subway.mapToStationTimeMapping)
        trip_time_count = trip_time_pair.combineByKey(
            lambda v: (v,1),
            lambda x,v: (x[0]+v,x[1]+1),
            lambda x,y: (x[0]+y[0],x[1]+y[1])
        )
        self.trip_average_time = trip_time_count.map(
            lambda (k,v): (k,float(v[0])/float(v[1]))
        )

    def saveAverageTripTime(self,output_file_path,local=False):
        if not local:
            self.trip_average_time.saveAsTextFile(output_file_path)
        else:
            result = self.trip_average_time.collect()
            print(result)

    def filterTripListByWalkingTimeStation(self,target_station):
        self.subway.initWalkingTimeStationLine(self.subway_station_line,target_station)
        #get end station trip
        start_target = self.trip_list.filter(self.subway.routeHandler.filterTripByPreviousStartAndTargetStation)\
            .map(lambda one_trip: ((one_trip.arriveTimeSlot(t_hour=3),one_trip.start.station_name,one_trip.end.station_name),one_trip))\
            .reduceByKey(self.subway.minimumTripByKey).map(lambda (a,b): b).flatMap(self.subway.mapToLineTarget)\
            .map(lambda ((one_line,target_station),one_trip):((one_line,target_station,one_trip.arriveTimeSlot(t_hour=3)),one_trip))

        target_start = self.trip_list.filter(self.subway.routeHandler.filterTripByTargetStationAndPreviousStation)\
            .map(lambda one_trip: ((one_trip.arriveTimeSlot(t_hour=3),one_trip.start.station_name,one_trip.end.station_name),one_trip))\
            .reduceByKey(self.subway.minimumTripByKey).map(lambda (a,b): b).flatMap(self.subway.mapToLineTarget)\
            .map(lambda ((one_line,target_station),one_trip):((one_line,target_station,one_trip.timeSlot(t_hour=3)),one_trip))


        target_end = self.trip_list.filter(self.subway.routeHandler.filterTripByTargetStationAndLatterStations)\
            .map(lambda one_trip: ((one_trip.arriveTimeSlot(t_hour=3),one_trip.start.station_name,one_trip.end.station_name),one_trip))\
            .reduceByKey(self.subway.minimumTripByKey).map(lambda (a,b): b).flatMap(self.subway.mapToLineTarget)\
            .map(lambda ((one_line,target_station),one_trip):((one_line,target_station,one_trip.timeSlot(t_hour=3)),one_trip))
        end_target = self.trip_list.filter(self.subway.routeHandler.filterTripByLatterStationAndTargetStation)\
            .map(lambda one_trip: ((one_trip.arriveTimeSlot(t_hour=3),one_trip.start.station_name,one_trip.end.station_name),one_trip))\
            .reduceByKey(self.subway.minimumTripByKey).map(lambda (a,b): b).flatMap(self.subway.mapToLineTarget)\
            .map(lambda ((one_line,target_station),one_trip):((one_line,target_station,one_trip.arriveTimeSlot(t_hour=3)),one_trip))

        start_end = start_target.join(target_end).union(end_target.join(target_start))
        two_station_trip_list = self.trip_list.filter(self.subway.routeHandler.filterTripByPreviousLatterSameLine).map(lambda one_trip: ((one_trip.arriveTimeSlot(t_hour=3),one_trip.start.station_name,one_trip.end.station_name),one_trip))\
            .reduceByKey(self.subway.minimumTripByKey).map(lambda (a,b): b)
        two_station_trip_time = two_station_trip_list.map(lambda one_trip: ((one_trip.start.station_name,one_trip.end.station_name,one_trip.timeSlot(t_hour=3)),one_trip.trip_time.total_seconds()))
        start_end_time = start_end.map(self.subway.mapTotalTripTimeByConnection)
        walking_time_trip_list = two_station_trip_time.join(start_end_time)
        time_slot_walking_time_list = walking_time_trip_list.filter(self.subway.filterByWalkingTime)\
            .map(lambda (k,v): (k[2],(v[1]-v[0])/2))
        time_slot_average_walking_time = time_slot_walking_time_list.combineByKey(lambda value: (value,1),
                                                                                  lambda x,value: (x[0]+value,x[1]+1),
                                                                                  lambda x,y: (x[0]+y[0],x[1]+y[1]))
        self.time_slot_average =  time_slot_average_walking_time.map(lambda (time_slot,(value_sum,count)): (time_slot,float(value_sum)/float(count)))
        self.time_slot_average_output = self.time_slot_average.map(lambda x: ",".join([str(x[0]),str(x[1])]))
        print(self.time_slot_average.collect())
        return(self.time_slot_average_output)


    def buildWaitingTimeInDistricts(self):
        self.subway.buildStationNameDistrictMapping('/media/zf72/Seagate Backup Plus Drive/E/DATA/edges/subway station/station_with_region.txt')
        local = self.user_trip_pair.collect()
        for (k,v) in local:
            print (k,v[0].start.district)
    def buildPureTripList(self):
        self.trip_list = self.trip_list.flatMap(self.subway.mapToPureTripList)
    def buildTripStartTravelTime(self):
        #self.trip_list.map(lambda)
        self.start_travel_time = self.trip_list.map(self.subway.mapToStartTravelTime)
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
class BusSpark(Spark):
    def __init__(self):
        self.bus = Bus()
        self.bus_smart_card_data = None
        self.connect()
        self.sc.addFile('data/shenzhen_tran_simple_gps.json')
        self.sc.addFile('data/shenzhen_district_simple_gps.json')

    def markSmartCardID(self,output_file_path):
        if self.bus_smart_card_data is None:
            self.filterSmartCardBusData()
        self.mask_bus_smart_card_data = self.bus_smart_card_data.map(self.bus.maskSmartCardID)
        self.mask_bus_smart_card_data.saveAsTextFile(output_file_path)

    def filterSmartCardBusData(self):
        self.bus_smart_card_data = self.input_data.filter(self.bus.filterBusSmartCardData)

    def buildSmartCardRecordList(self):
        self.smart_card_record_list = self.input_data.map(self.bus.parseOneRowToRecord).filter(lambda x: x is not None)#.map(lambda record: print(record.station_id))

    def buildSmartCardUserTripList(self):
        self.user_record_list = self.smart_card_record_list.map(lambda x: (x.user_id,x)).groupByKey()
    def buildRecordList(self):
        trans_region_file_path = SparkFiles.get("shenzhen_tran_simple_gps.json")
        self.bus.initPointRegionMapping(trans_region_file_path)
        dist_region_file_path = SparkFiles.get("shenzhen_district_simple_gps.json")
        self.bus.initPointDistrictMapping(dist_region_file_path)
        self.record_list = self.input_data.map(self.bus.parseRecord).filter(lambda record: record is not None) #and record.is_occupied)

    def buildTripList(self):
        record_group_user = self.record_list.groupBy(lambda record: record.plate)
        sorted_record_group_user = record_group_user.mapValues(list).map(lambda (k,v): (k, sorted(v, key=lambda record: record.time)))
        self.user_bus_trip_list = sorted_record_group_user.map(self.bus.parseRecordTotrip).filter(lambda (p,trip_list): len(trip_list) > 0)

    def buildTripTimeGPSListForMapMatching(self):
        self.trip_route_time_gps  = self.user_bus_trip_list.flatMap(self.bus.trip_to_time_gps_point)

    def buildPureTripList(self):
        self.trip_list  = self.user_bus_trip_list.flatMap(self.bus.toTripList)

    def filterTripByGPSStation(self,start_station_gps=None,arrive_station_gps=None,filter_by= 'or'):
        self.bus.start_station_gps = start_station_gps
        self.bus.arrive_station_gps = arrive_station_gps
        self.bus.filter_by = filter_by
        self.filtered_trip_list = self.trip_list.filter(self.bus.filterTripByGPSStation)


    def buildRouteTripList(self):
        # self.filter_trip_list.
        # self.trip_route_time_gps.groupBy()
        self.route_trip_list = self.filtered_trip_list.map(lambda x: (x.start.route_num,x)).groupByKey().map(self.bus.sortOneRouteBusByTime)#.groupBy() #.map(self.bus.groupByRouteSortedByTime)
        self.start_arrive_time_list = self.route_trip_list.map(self.bus.mapStartArriveTime)

        # result = self.start_arrive_time_list.collect()
        # for one_record in result:
        #     print(one_record)
    def buildBusRouteNames(self):
        self.station_names = self.record_list.map(lambda x: (x.route_num,x)).groupByKey().map(lambda x:x[0])


    def filterByRouteName(self):
        routes = ['0320','0326','0395','E18','K318','M363','M413','M414','M435','M447','M448','GF119','1C','SG']
        self.bus.routes = routes
        self.record_list = self.record_list.filter(self.bus.filterRecordByRouteNames)



class TaxiSpark(Spark):
    def __init__(self):
        self.taxi = Taxi()
        self.connect()
        self.sc.addFile('data/shenzhen_tran_simple_gps.json')
        self.sc.addFile('data/shenzhen_district_simple_gps.json')

    def buildRecordList(self):
       trans_region_file_path = SparkFiles.get("shenzhen_tran_simple_gps.json")
       self.taxi.initPointRegionMapping(trans_region_file_path)
       dist_region_file_path = SparkFiles.get("shenzhen_district_simple_gps.json")
       self.taxi.initPointDistrictMapping(dist_region_file_path)
       self.record_list = self.input_data.map(self.taxi.parseRecord).filter(lambda record: record is not None) #and record.is_occupied)

    def buildTripList(self):
        record_group_user = self.record_list.groupBy(lambda record: record.plate)
        sorted_record_group_user = record_group_user.mapValues(list).map(lambda (k,v): (k, sorted(v, key=lambda record: record.time)))
        self.user_taxi_trip_list = sorted_record_group_user.map(self.taxi.parseRecordTotrip)

    def buildTripTimeGPSListForMapMatching(self):
        filter_trip_list = self.user_taxi_trip_list.filter(lambda (k,v): len(v) > 0)
        self.trip_route_time_gps  = filter_trip_list.flatMap(self.taxi.trip_to_time_gps_point)
        # trip_list = self.trip_route_time_gps.collect()
        # for one_record in trip_list:
        #     print(one_record)

    def buildODTravelTime(self):
        self.od_trip_user = self.user_taxi_trip_list.flatMap(self.taxi.userTripToTripUser)
        od_time_mapping = self.od_trip_user.map(lambda (k,v): (k,v[0].trip_time.total_seconds()))
        self.od_minimum_time = od_time_mapping.reduceByKey(min)


    def buildDelayTimeDistribution(self):
        trip_delay_time = self.od_trip_user.join(self.od_minimum_time).map(lambda (k,v): (v[0][0].timeSlot(t_hour=1),k,v[0][0].trip_time.total_seconds()-v[1]))
        delay_time = trip_delay_time.map(lambda (t,l,d):(t,d)).combineByKey(lambda value: (value,1),
                                                                                    lambda x,value: (x[0]+value,x[1]+1),
                                                                                    lambda x,y: (x[0]+y[0],x[1]+y[1]))
        self.average_delay_time =  delay_time.map(lambda (t,(value_sum,count)): (t,float(value_sum)/count))
        return self.average_delay_time

    def setStartFilterDistrictName(self,district_name):
        self.taxi.start_district = district_name
    def setEndFilterDistrictName(self,district_name):
        self.taxi.end_district = district_name

    def filterTripByStartDistrict(self,district_name = None):
        if district_name is not None:
            self.taxi.start_district = district_name
        self.od_trip_user = self.od_trip_user.filter(self.taxi.filterByStartDistrict)

    def filterTripByEndDistrict(self,district_name = None):
        if district_name is not None:
            self.taxi.end_district = district_name
        self.od_trip_user = self.od_trip_user.filter(self.taxi.filterByEndDistrict)


    def test(self):
        # mapping = self.sc.textFile("/zf72/data/edges/shenzhen_tran_simple_gps.json").cache()
        # t = mapping.map(json.loads).collect()
        test = json.load(open('../data/shenzhen_tran_simple_gps.json'))
        print(test.keys())



class PVSpark(Spark):
    def __init__(self):
        self.pv  = PV()
        self.connect()
        self.sc.addFile('data/shenzhen_tran_simple_gps.json')
        self.sc.addFile('data/shenzhen_district_simple_gps.json')

    def buildRecordList(self):
        trans_region_file_path = SparkFiles.get("shenzhen_tran_simple_gps.json")
        self.pv.initPointRegionMapping(trans_region_file_path)
        dist_region_file_path = SparkFiles.get("shenzhen_district_simple_gps.json")
        self.pv.initPointDistrictMapping(dist_region_file_path)
        self.record_list = self.input_data.map(self.pv.parseRecord).filter(lambda record: record is not None) #and record.is_occupied)

    def buildTripList(self):
        record_group_user = self.record_list.groupBy(lambda record: record.pv_id)
        sorted_record_group_user = record_group_user.mapValues(list).map(lambda (k,v): (k, sorted(v, key=lambda record: record.time)))
        self.user_taxi_trip_list = sorted_record_group_user.map(self.pv.parseRecordTotrip).filter(lambda (k,v): len(v) > 0)

        # for one_record_list in self.user_taxi_trip_list.collect():
        #     print(one_record_list[0])
        #     for one_record in one_record_list[1]:
        #         print(one_record.start.time,one_record.end.time)


    def buildODTravelTime(self):
        self.od_trip_user = self.user_taxi_trip_list.flatMap(self.pv.userTripToTripUser)
        od_time_mapping = self.od_trip_user.map(lambda (k,v): (k,v[0].trip_time.total_seconds()))
        self.od_minimum_time = od_time_mapping.reduceByKey(min)


    def buildDelayTimeDistribution(self):
        trip_delay_time = self.od_trip_user.join(self.od_minimum_time).map(lambda (k,v): (v[0][0].timeSlot(t_hour=1),k,v[0][0].trip_time.total_seconds()-v[1]))
        delay_time = trip_delay_time.map(lambda (t,l,d):(t,d)).combineByKey(lambda value: (value,1),
                                                                                    lambda x,value: (x[0]+value,x[1]+1),
                                                                                    lambda x,y: (x[0]+y[0],x[1]+y[1]))
        self.average_delay_time =  delay_time.map(lambda (t,(value_sum,count)): (t,float(value_sum)/count))
        return self.average_delay_time

    def setStartFilterDistrictName(self,district_name):
        self.pv.start_district = district_name
    def setEndFilterDistrictName(self,district_name):
        self.pv.end_district = district_name

    def filterTripByStartDistrict(self,district_name = None):
        if district_name is not None:
            self.pv.start_district = district_name
        self.od_trip_user = self.od_trip_user.filter(self.pv.filterByStartDistrict)

    def filterTripByEndDistrict(self,district_name = None):
        if district_name is not None:
            self.pv.end_district = district_name
        self.od_trip_user = self.od_trip_user.filter(self.pv.filterByEndDistrict)


    def demandDistributionOneDay(self):
        self.time_slot_num = self.user_taxi_trip_list.flatMap(self.pv.mapToTimeSlot).groupByKey().mapValues(sum)
        taxi_trip_list = self.time_slot_num.collect()
        for one_record in taxi_trip_list:
            print(one_record)



