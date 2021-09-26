#coding=utf-8
from Assistant import *
from TripEntity import *
from Route import *
class Interval():
    def __init__(self):
        self.init()
        self.file_path = None
        self.dir_path = None
        self.record_list = None
        self.all_trips = None
        self.time_matrix = None
        self.in_vehicle_time = None
        self.start_station = None
        self.end_station = None

    def init(self):
        self.time_assist = TimeAssistant()

    def subwayOneTrip(self,row):
        attrs = row.split(',')
        recordTimeIn = self.time_assist.parseTime(attrs[4])
        recordTimeOut = self.time_assist.parseTime(attrs[14])
        recordIn = SubwayRecord(user_id=attrs[1],time=recordTimeIn,in_station=True,station_id=attrs[6],station_name=attrs[6],route_name=attrs[5])
        recordOut = SubwayRecord(user_id=attrs[11],time=recordTimeOut,in_station=False,station_id=attrs[16],station_name=attrs[16],route_name=attrs[15])
        return([recordIn,recordOut])

    def parseOneSubwayRow(self,row):
        try:
            if len(row.split(',')) > 20:
                return(self.subwayOneTrip(row))
            elif len(row.split(',')) == 8:
                return([self.subwayShortRecord(row)])
            elif len(row.split(',')) == 7:
                return([self.subwayTinyRecord(row)])
            else:
                return([self.subwayOneRecord(row)])
        except Exception as e:
            print(e)
            return([None])
    # def subwayTinyRecord(self,row):
    #     attrs = row.split(",")
    #
    #     else:
    #         return(None)
    #     time_str = attrs[4].replace("T"," ").rstrip('Z').rstrip('.000')
    #     recordTime = self.time_assist.parseTime(time_str)
    #     record = SubwayRecord(user_id=attrs[1],time=recordTime,in_station=in_station,station_id=attrs[6],station_name=attrs[6],route_name=attrs[5],train_id=attrs[7])
    #     return(record)

    def subwayShortRecord(self,row):
        attrs = row.split(",")
        if attrs[3] == '22':
            in_station = False
        elif attrs[3] == '21':
            in_station = True
        elif attrs[3] == "地铁入站":
            in_station =True
        elif attrs[3] == "地铁出站":
            in_station = False
        else:
            return(None)
        time_str = attrs[4].replace("T"," ").rstrip('Z').rstrip('.000')
        recordTime = self.time_assist.parseTime(time_str)
        record = SubwayRecord(user_id=attrs[1],time=recordTime,in_station=in_station,station_id=attrs[6],station_name=attrs[6],route_name=attrs[5],train_id=attrs[7])
        return(record)
    def subwayOneRecord(self,row):
        attrs = row.split(",")
        if attrs[4] == '22':
            in_station = False
        elif attrs[4] == '21':
            in_station = True
        elif attrs[4] == "地铁入站":
            in_station =True
        elif attrs[4] == "地铁出站":
            in_station =False
        else:
            return None
        recordTime = self.time_assist.parseTime(attrs[8])
        if len(attrs)>=15:
            record = SubwayRecord(user_id=attrs[1],time=recordTime,in_station=in_station,station_id=attrs[13],station_name=attrs[13],route_name=attrs[12],train_id=attrs[14])
        else:
            record = SubwayRecord(user_id=attrs[1],time=recordTime,in_station=in_station,station_id=attrs[13],station_name=attrs[13],route_name=attrs[12])
        return(record)


    def initSubwayMapping(self,file_path):
        route = SubwayRouteHandler()
        route.buildStationLineMap(file_path)
        route.buildStationLineMap(file_path)

    def parseTuple(self,one_row):
        row =one_row.strip("(").strip(")")
        attrs = row.split(",")
        r = [v.strip(" ").strip('"').strip("'").strip('"') for v in attrs]
        r = [v.strip("\'") for v in r]
        return(r)

    def subwayOneRow(self,row):
        attrs = row.split(",")
        if attrs[4] == '22':
            in_station = False
        elif attrs[4] == '21':
            in_station = True
        elif attrs[4] == "地铁入站":
            in_station =True
        elif attrs[4] == "地铁出站":
            in_station =False
        else:
            return None
        recordTime = self.parseTime(attrs[8])
        if len(attrs) == 15:
            record = SubwayRecord(user_id=attrs[1],time=recordTime,in_station=in_station,station_id=attrs[13],station_name=attrs[13],route_name=attrs[12],train_id=attrs[14])
        else:
            record = SubwayRecord(user_id=attrs[1],time=recordTime,in_station=in_station,station_id=attrs[13],station_name=attrs[13],route_name=attrs[12])
        return record
    def generateUniqueSubwayMap(self):
        if self.file_path is None:
            print "ERROR: set file path first"
            return None
        with open(self.file_path) as csvFile:
            for one_row in csvFile:
                record = self.subwayOneRow(one_row)
                if record is not None:
                    print record.in_station
    def inSubwayTime(self):
        if self.file_path is None:
            print "ERROR: set file path first"
            return None
        with open(self.file_path) as csvFile:
            for one_row in csvFile:
                pass

    def buildRecordList(self):
        self.record_list = {}
        if self.file_path is None:
            print "ERROR: set file path first"
            return None
        with open(self.file_path) as csvFile:
            for one_row in csvFile:
                record = None
                try:
                    record = self.subwayOneRow(one_row)
                except:
                    record = None
                if record is not None:
                    if record.user_id not in self.record_list.keys():
                        self.record_list[record.user_id] = [record]
                    else:
                        self.record_list[record.user_id].append(record)
        return self.record_list

    def buildTripList(self):
        if self.record_list is None:
            print "INFO: build station list first, call buildRecordList"
            return None
        self.all_trips = []
        for one_user in self.record_list.keys():
            start = None
            end = None
            one_user_records = self.record_list[one_user]
            sorted_records = sorted(one_user_records, key=lambda record: record.time)
            for record in sorted_records:
                if record.in_station:
                    start = record
                else:
                    end = record
                    if start is not None:
                        temp_trip = Trip(start=start,end=end)
                        temp_trip.start_time = start.time
                        temp_trip.end_time = end.time
                        temp_trip.computeTripTime()
                        self.all_trips.append(temp_trip)
                        start =None
    def buildTripTimeMatrix(self):
        if self.all_trips is None:
            print "INFO: build trip list first, call buildTripList"
            return None
        self.time_matrix = {}
        for one_trip in self.all_trips:
            start_end = one_trip.start.station_id+","+one_trip.end.station_id
            timeslot = one_trip.timeSlot(t_min=60)
            if start_end not in self.time_matrix.keys():
                self.time_matrix[start_end] = [one_trip]
            else:
                self.time_matrix[start_end].append(one_trip)
    def inVehicleTimeEstiamte(self):
        if self.time_matrix is None:
            print "INFO: build time matrix first, call buildTripTimeMatrix"
            return None
        self.in_vehicle_time = {}
        for one_key in self.time_matrix.keys():
            min_trip_time =  -1
            trip_list = self.time_matrix[one_key]
            for one_trip in trip_list:
                if min_trip_time == -1:
                    min_trip_time = one_trip.trip_time
                elif min_trip_time > one_trip.trip_time:
                    min_trip_time = one_trip.trip_time
            self.in_vehicle_time[one_key] = min_trip_time

    def filterWaitingTimeByStartEndStation(self,one_trip):
        ((start_station,end_station),(trip,in_vechile_time)) = one_trip
        return(self.filterByStartEndStation(trip))


    def waitingTimeOneDay(self):
        self.buildRecordList()
        self.buildTripList()
        self.buildTripTimeMatrix()
        self.inVehicleTimeEstiamte()
        self.one_day_waiting_time = {}
        self.one_day_average_waiting_time = {}
        for one_key in self.time_matrix.keys():
            trip_list = self.time_matrix[one_key]
            for one_trip in trip_list:
                one_trip.waitingTime(self.in_vehicle_time[one_key])
                if one_trip.timeslot not in self.one_day_waiting_time.keys():
                    self.one_day_waiting_time[one_trip.timeslot] = [one_trip.waiting_time_to_min]
                else:
                    self.one_day_waiting_time[one_trip.timeslot].append(one_trip.waiting_time_to_min)
        for one_key in self.one_day_waiting_time.keys():
            trip_list = self.one_day_waiting_time[one_key]
            self.one_day_average_waiting_time[str(one_key)] = float(sum(trip_list))/float(len(trip_list))
        with open('../data/average_waiting_time.json','w') as fh:
            json.dump(self.one_day_average_waiting_time,fh)

    def userTripToNoUserTripList(self,user_trip_list):
        (user,trip_list) = user_trip_list
        all_trips = []
        for one_trip in trip_list:
            one_trip.user_id = user
            all_trips.append(one_trip)
        return(all_trips)

    def filterByStartEndStation(self,one_trip):
        if self.start_station is None and self.end_station is None:
            return(True)
        elif self.start_station is not None and self.end_station is not None:
            return((self.start_station== one_trip.start.station_name and self.end_station == one_trip.end.station_name) or (self.start_station.rstrip('站') == one_trip.start.station_name.rstrip('站') and self.end_station.rstrip('站') == one_trip.end.station_name.rstrip('站')))
        elif self.end_station is not None:
            return(self.end_station  == one_trip.end.station_name or self.end_station.rstrip('站') == one_trip.end.station_name.rstrip('站'))
        elif self.start_station is not None:
            return(self.start_station == one_trip.start.station_name or self.start_station.rstrip('站') == one_trip.start.station_name.rstrip('站'))
        # elif self.start_station is not None and self.end_station is not None:
        #     return(self.start_station.rstrip('站') == one_trip.start.station_name.rstrip('站') and self.end_station.rstrip('站') == one_trip.end.station_name.rstrip('站'))
        # elif self.end_station is not None:
        #     return(self.end_station.rstrip('站') == one_trip.end.station_name.rstrip('站'))
        # elif self.start_station is not None:
        #     return(self.start_station.rstrip('站') == one_trip.start.station_name.rstrip('站'))

    def fitlerByStartEndStationByTrip(self,one_record):
        one_trip = one_record
        return(self.filterByStartEndStation(one_trip))

    def buildStationNameDistrictMapping(self,mapping_file_path):
        self.station_districts  = {}
        with open(mapping_file_path) as file_handler:
            content = file_handler.read()
            for one_line in  content:
                attrs = one_line.split(',')
                station_name = attrs[0]
                district_name = attrs[3]
                self.station_districts[station_name] = district_name
        return self.station_districts

    def stationDistrictMappingOneLine(self,one_line):
        attrs = one_line.split(',')
        station_name = attrs[0]
        district_name = attrs[3]
        return (station_name,district_name)

    def mapStationNameToDistrict(self,station_name):
        if self.station_districts is None:
            print "ERROR: %s" % "station mapping is none, call buildStationNameDistrictMapping"
            return None
        return self.station_districts[station_name]


#*********************************for spark use
    def sortedRecordsToTrip(self,input):
        start = None
        end = None
        (k,sorted_records) = input
        all_trips = []
        for record in sorted_records:
                if record.in_station:
                    start = record
                else:
                    end = record
                    if start is not None and start is not None:
                        temp_trip = Trip(start=start,end=end)
                        temp_trip.start_time = start.time
                        temp_trip.end_time = end.time
                        temp_trip.computeTripTime()
                        #temp_trip.setDistrictByStationName(self)
                        if start.station_id != end.station_id:
                            all_trips.append(temp_trip)
                        start =None
                        end = None
        return (k,all_trips)


    def mapToStationTimeMapping(self,user_trip_list):
        (user_id,trip_list) = user_trip_list
        od_time = []
        for one_trip in trip_list:
            od_time.append((one_trip.originDestination(),one_trip.trip_time.total_seconds()))
        return od_time
    def mapToODTrip(self,user_trip_list):
        (user_id,trip_list) = user_trip_list
        od_time = []
        for one_trip in trip_list:
            od_time.append((one_trip.originDestination(),one_trip))
        return od_time
    def mapToTripWaitingTime(self,user_trip_in_vehicle_time):
        (od,(one_trip,in_vehicle_time)) = user_trip_in_vehicle_time
        return (one_trip.timeSlot(t_sec=1), one_trip.start.time,one_trip.trip_time.total_seconds() - in_vehicle_time)

    def tripClusterWithRegion(self,id_trip_list):
        (user_id, trip_list) = id_trip_list
        for one_trip in trip_list:
            print one_trip.start

    def user_trip_to_trip_user(self,input):
        (user_id,trip_list) = input
        trip_userids = []
        for one_trip in trip_list:
            trip_userids.append((user_id,one_trip))
        return trip_userids

    def mapToPureTripList(self,one_record):
        (user_id,trip_list) = input
        return(trip_list)

    def initWalkingTimeStationLine(self,file_path,target_station):
        self.routeHandler = SubwayRouteHandler()
        self.routeHandler.setTargetStation(target_station=target_station)
        self.routeHandler.buildRoutes(file_path=file_path)
        self.routeHandler.buildStationLineMap(file_path=file_path)
        self.routeHandler.buildTargetLatterStations()
        self.routeHandler.buildTargetPreviousStations()
        self.routeHandler.buildTargetStationPairs()

    def mapToLineTarget(self,one_trip):
        start_lines = self.routeHandler.station_line_mapping[one_trip.start.station_name]
        end_lines = self.routeHandler.station_line_mapping[one_trip.end.station_name]
        intersect_lines = list(set(start_lines).intersection(end_lines))
        output = []
        for one_line in intersect_lines:
            output.append(((one_line,self.routeHandler.target_station),one_trip))
        return(output)

    def minimumTripByKey(self,a,b):
        if a.trip_time.total_seconds() > b.trip_time.total_seconds():
            return(b)
        else:
            return(a)

    def mapTotalTripTimeByConnection(self,one_record):
        (one_key,one_value) = one_record
        (line,station_name,time_slot) = one_key
        (start_trip,end_trip) = one_value
        total_trip_time = (start_trip.trip_time + end_trip.trip_time).total_seconds()
        output_key = (start_trip.start.station_name,end_trip.end.station_name,time_slot)
        output_value = total_trip_time
        return((output_key,output_value))

    def filterByWalkingTime(self,one_record):
        (k,v) = one_record
        if v[1] > v[0]+20:
            return(True)
        else:
            return(False)

    def mapToStartTravelTime(self,one_trip):
        result = []
        result.append(TimeAssistant().parseTimeToStr(one_trip.start.time))
        result.append(str((one_trip.end.time-one_trip.start.time).total_seconds()))
        result.append(TimeAssistant().parseTimeToStr(one_trip.end.time))
        result.append(one_trip.start.station_name)
        result.append(one_trip.end.station_name)
        return(",".join(result))

    def setDistrictAfterJoin(self,key_values):
        (district,record) = key_values
        record[0].district = record[1]
        return(record[0])

    def mapToTripDelayRatio(self,key_values):
        ((start_district,end_district),((time_slot,travel_time),min_travel_time)) = key_values
        if min_travel_time == 0:
            delay_ratio = 0
        else:
            delay_ratio = float(travel_time - min_travel_time)/float(min_travel_time)
        return(start_district,end_district,time_slot,delay_ratio)


