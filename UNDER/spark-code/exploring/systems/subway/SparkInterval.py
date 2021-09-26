 #coding=utf-8
from Interval import *
from Assistant import *
class SparkInterval():
    def init(self):
        self.interval = Interval()
        self.subway = self.interval


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
        subway_record_list = input_data.flatMap(self.subway.parseOneSubwayRow).filter(lambda x: x is not None)#.map(lambda record: print(record.station_id))
        self.subway_record_list = subway_record_list
        user_record_list = subway_record_list.map(lambda x: (x.user_id,x)).groupByKey()
        self.sorted_user_record_list = user_record_list.mapValues(list).filter(lambda (k,v): len(v) > 1).map(lambda (k,v): (k, sorted(v, key=lambda record: record.time)))
    def buildRawRecordList(self,input_data):
        '''
        build the record list for each user and sort it by the time
        :return:
        '''
        subway_record_list = input_data.flatMap(self.subway.parseOneSubwayRow).filter(lambda x: x is not None)#.map(lambda record: print(record.station_id))
        self.subway_record_list = subway_record_list

    def buildStationNumber(self):
        one_file = self.input_data
        subway_record_list = one_file.map(self.subway.subwayOneRow).filter(lambda x: x is not None)
        stations = subway_record_list.map(lambda x: (SubwayRecord(x).station_name,1)).groupByKey()
        return(stations)

    def buildPolygonMapping(self,polygon_file):
        station_polygon_mapping = self.sc.textFile(polygon_file,use_unicode=False).cache()
        station_district_mapping = station_polygon_mapping.map(self.subway.stationDistrictMappingOneLine)
        station_district_mapping =station_district_mapping.map(lambda (k,v): (v,k)).filter(lambda (k,v): '地铁站' in v)
        station_district_mapping = station_district_mapping.map(lambda (k,v): (v.replace('地铁站',''),k))
        self.station_polygon_mapping  = station_district_mapping


    def mapRecordsToPolygon(self):
        one_file = self.input_data
        subway_record_list = one_file.flatMap(self.subway.parseOneSubwayRow).filter(lambda x: x is not None)#.map(lambda record: print(record.station_id))
        print(subway_record_list.count())
        subway_record_list = subway_record_list.map(lambda record: (record.station_name.rstrip('站'),record))
        records_after_filter = subway_record_list.join(self.station_polygon_mapping).map(self.subway.setDistrictAfterJoin)#.map(lambda (k,v): v[0])
        user_record_list = records_after_filter.map(lambda x: (x.user_id,x)).groupByKey()
        self.sorted_user_record_list = user_record_list.mapValues(list).filter(lambda (k,v): len(v) > 1).map(lambda (k,v): (k, sorted(v, key=lambda record: record.time)))

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
        #self.in_vehicle_time.saveAsTextFile('/zf72/transportation_data/result/in_vehicle_time/0601')

    def buildWaitingTimeInTwoStations(self,start_station=None,end_station=None):
        self.setStartEndStation(start_station,end_station)
        user_trips_in_vehicle_time = self.user_trip_pair.flatMap(self.subway.mapToODTrip).join(self.in_vehicle_time)
        filter_user_trips_in_vehicle_time = user_trips_in_vehicle_time.filter(self.subway.filterWaitingTimeByStartEndStation)

        self.timeslot_waiting_time = filter_user_trips_in_vehicle_time.map(self.subway.mapToTripWaitingTime).map(lambda (k,v1,v2): (k,v2))
        return(self.timeslot_waiting_time)

    def buildODStartVsTravelTime(self):
        self.od_starttime__traveltime = self.trip_list.map(lambda x: ((x.start.district,x.end.district),(x.timeSlot(t_min=5),x.computeTripTimeToSeconds())))
        self.min_od_traveltime = self.trip_list.map(lambda x:((x.start.district,x.end.district), x.computeTripTimeToSeconds())).reduceByKey(min)
        join_od_starttime_traveltime = self.od_starttime__traveltime.join(self.min_od_traveltime)
        self.delay_ratio = join_od_starttime_traveltime.map(self.subway.mapToTripDelayRatio)
        #print(self.delay_ratio.collect())
        return(self.delay_ratio)
        # print(delay_ratio.collect())
        # trip_list = self.trip_list.map(lambda x: x.start.district).collect()
        # print(trip_list)
    def buildODStartVsTravelTimeStationToStation(self):
        self.od_starttime__traveltime = self.trip_list.map(lambda x: ((x.start.station_name,x.end.station_name),(x.timeSlot(t_min=5),x.computeTripTimeToSeconds())))
        self.min_od_traveltime = self.trip_list.map(lambda x:((x.start.station_name,x.end.station_name), x.computeTripTimeToSeconds())).reduceByKey(min)
        join_od_starttime_traveltime = self.od_starttime__traveltime.join(self.min_od_traveltime)
        self.delay_ratio = join_od_starttime_traveltime.map(self.subway.mapToTripDelayRatio)
        #print(self.delay_ratio.collect())
        return(self.delay_ratio)

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
        return(self.time_slot_average)


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

