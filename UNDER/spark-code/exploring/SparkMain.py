#coding=utf-8
from Spark import *
import time
from hdfs import Config
class SubwayMain():
    def __init__(self):
        self.subway = SubwaySpark()
        self.subway_file_dir = "/zf72/transportation_data/subway/input/"
        self.subway_file_prefix = "P_GJGD_SZT_"
        self.subway_output_dir = "/zf72/transportation_data/subway/output/%s/%s_%s/%s"
        self.subway_output_dir_pure = "/zf72/transportation_data/subway/output/"

    def subwayWaitingTime(self):
        subway = self.subway
        test_file = [('a',44),('a',10),('a',11),('a',12),('a',5),('c',3),('b',2),('a',1),('a',6),('a',3)]
        subway.setLocalInputFile(test_file)
        subway.buildRecordList()

    def waitingTimeInCity(self):
        subway = self.subway
        subway.setHDFSFilePath('/zf72/transportation_data/subway/input/P_GJGD_SZT_20160601')
        #subway.setLocalFilePath("/home/zf72/Dropbox/projects/off-peak-trans/data/SZT/P_GJGD_SZT_20160601")
        #subway.setLocalFilePath("/home/zf72/Dropbox/projects/off-peak-trans/data/sample/public_sample.txt")
        subway.buildRecordList()
        subway.buildTripList()
        # subway.buildTripTimeMatrix()
        subway.buildInVehicleTime()
        subway.buildWaitingTime("whole_city")
        #subway.saveAverageTripTime('/zf72/transportation_data/result/P_GJGD_SZT_20160601',local=False)
        #subway.saveAverageTripTime("file:/home/zf72/Dropbox/projects/off-peak-trans/data/SZT_RESULT/P_GJGD_SZT_20160601",local=True)

    def waitingTimeFilterByStartDistrict(self,start_district,subway=None):
        if subway is None:
            subway = SubwaySpark()
        subway.setHDFSFilePath('/zf72/transportation_data/subway_bus/P_GJGD_SZT_20160601')
        # subway.setLocalFilePath("/home/zf72/Dropbox/projects/off-peak-trans/data/sample/public_sample.txt")
        subway.buildRecordList()
        subway.buildTripList()
        subway.buildInVehicleTime()
        subway.buildDistrictFilter(start_district)
        subway.filterTripListByStartDistrict()
        subway.buildWaitingTime("from_"+start_district)

    def waitingTimeFilterByDestinationDistrict(self,end_district,subway=None):
        if subway is None:
            subway = SubwaySpark()
        subway.setHDFSFilePath('/zf72/transportation_data/subway_bus/P_GJGD_SZT_20160601')
        # subway.setLocalFilePath("/home/zf72/Dropbox/projects/off-peak-trans/data/sample/public_sample.txt")
        subway.buildRecordList()
        subway.buildTripList()
        subway.buildInVehicleTime()
        subway.buildDistrictFilter(end_district)
        subway.filterTripListByDestinationDistrict()
        subway.buildWaitingTime("to_"+end_district)

    def waitingTimeByDistricts(self):
        districts = ['Luohu','Futian','Nanshan','Longgang','Baoan','Yantian']
        subway = SubwaySpark()
        for one_district in districts:
            self.waitingTimeFilterByStartDistrict(one_district,subway)
            self.waitingTimeFilterByDestinationDistrict(one_district,subway)

    def waitingTimeInTwoStations(self):
        start_station = "世界之窗"
        end_station = "华侨城"
        file_name = 'P_GJGD_SZT_20160601'
        type_name = "waiting_time"
        # self.subway.setHDFSFilePath("/zf72/transportation_data/sample/input/SZT_sample_0601.txt")
        self.subway.setHDFSFilePath("/zf72/transportation_data/subway/input/P_GJGD_SZT_20160601")
        self.subway.buildRecordList()
        self.subway.buildTripList()
        self.subway.buildInVehicleTime()
        time_waiting_time = self.subway.buildWaitingTimeInTwoStations(start_station=start_station,end_station=end_station)
        time_waiting_time.saveAsTextFile('/zf72/transportation_data/subway/output/'+file_name+'/'+start_station+'_'+end_station+'_'+type_name)

    def waitingTimeFilterByStartStation(self):
        # start_station = "双龙"
        # start_station = "坂田"
        start_station = "福田"
        end_station =None
        days = [1,2,3,4,5,6,7]
        for ii in days:
            file_name = 'P_GJGD_SZT_2016060'+str(ii)
            type_name = "waiting_time"
            # self.subway.setHDFSFilePath("/zf72/transportation_data/sample/input/SZT_sample_0601.txt")
            self.subway.setHDFSFilePath("/zf72/transportation_data/subway/input/%s" % file_name)
            self.subway.buildRecordList()
            self.subway.buildTripList()
            self.subway.buildInVehicleTime()
            time_waiting_time = self.subway.buildWaitingTimeInTwoStations(start_station=start_station,end_station=end_station)
            time_waiting_time.saveAsTextFile('/zf72/transportation_data/subway/output/'+file_name+'/'+start_station+'_'+type_name)





    def travelTimeBetweenTwoStation(self):
        start_station = "罗湖站"
        end_station = "深圳北站"
        start_output_station = 'LuoHu'
        end_output_station = 'ShenzhenBei'
        # start_station = None
        # end_station = None
        type_name = "travel_time"
        client = Config().get_client('dev')
        days = client.list(self.subway_file_dir)

        # days = [['2012120*'],['2012121*'],['2012122*'],['2012123*'],['201606*']]
        # # self.subway.setHDFSFilePath("/zf72/transportation_data/sample/input/SZT_sample_0601.txt")
        # # self.subway.setHDFSFilePath("/zf72/transportation_data/subway/input/"+file_name)
        result = {}
        ii = 0
        for day in days:
            day = [day]
            files = [self.subway_file_dir+one_file for one_file in day]
            file_path = ','.join(files)
            # self.subway.setHDFSFilePath("/zf72/transportation_data/sample/input/SZT_sample_0601.txt")
            self.subway.setHDFSFilePath(file_path)
            self.subway.buildRecordList()
            self.subway.buildTripList()
            self.subway.buildNoUserTripList()
            self.subway.filterTripListByStartEndStation(start_station,end_station)
            trip_time = self.subway.tripStartTimeVsTravelTime()
            self.subway.average_trip_time = self.subway.buildAverageByKey(trip_time)
            # output_file_dir = self.subway_output_dir % (type_name, start_output_station, end_output_station, day[0])
            # self.subway.average_trip_time.saveAsTextFile(output_file_dir)
            one_result = self.subway.average_trip_time.collect()
            result[day[0]] = one_result
            ii += 1
            if ii ==3 :
                break
        json.dump(result,open('../data_sync/spark-results/travel-time-Luohu-shenzhenbei-24hours.json','w'))


    def riddingTimeBetweenTwoStation(self):
        start_station = "福田"
        # end_station = "深圳北站"
        end_station = "横岗"
        file_name = 'P_GJGD_SZT_20160601'
        type_name = "ridding_time"
        # self.subway.setHDFSFilePath("/zf72/transportation_data/sample/input/SZT_sample_0601.txt")
        self.subway.setHDFSFilePath("/zf72/transportation_data/subway/input/"+file_name)
        self.subway.buildRecordList()
        self.subway.buildTripList()
        self.subway.tripStartTimeVsRiddingTime(start_station=start_station,end_station=end_station)
        self.subway.time_slot_ridding_time.saveAsTextFile('/zf72/transportation_data/subway/output/'+type_name+'/'+start_station+'_'+end_station)
        # time_waiting_time = self.subway.buildWaitingTimeInTwoStations(start_station=start_station,end_station=end_station)
        # time_waiting_time.saveAsTextFile('/zf72/transportation_data/subway/output/'+file_name+'/'+start_station+'_'+end_station+'_'+type_name)

        # self.subway.filterTripListByStartEndStation(start_station,end_station)
        # trip_time = self.subway.tripStartTimeVsTravelTime()
        # trip_time.saveAsTextFile('/zf72/transportation_data/subway/output/'+file_name+'/'+start_station+'_'+end_station+"_ridding_time")

    def riddingTimeMultipleDayBetweenTwoStations(self):
        start_station = "福田"
        # end_station = "深圳北站"
        end_station = "横岗"
        file_name_prefix = 'P_GJGD_SZT_2016060'
        type_name = "ridding_time"
        days = [1,2,3,4,5,6,7]
        for one_day in days:
            file_name = file_name_prefix + str(one_day)
            self.subway.setHDFSFilePath("/zf72/transportation_data/subway/input/"+file_name)
            self.subway.buildRecordList()
            self.subway.buildTripList()
            self.subway.tripStartTimeVsRiddingTime(start_station=start_station,end_station=end_station)
            self.subway.time_slot_ridding_time.saveAsTextFile('/zf72/transportation_data/subway/output/'+type_name+'/'+start_station+'_'+end_station+"/"+file_name)

    def walkingTimeEstimation(self):
        target_station = "横岗"
        file_name = 'P_GJGD_SZT_20160601'
        # self.subway.setHDFSFilePath("/zf72/transportation_data/sample/input/SZT_sample_0601.txt")
        self.subway.setHDFSFilePath("/zf72/transportation_data/subway/input/"+file_name)
        self.subway.buildRecordList()
        self.subway.buildTripList()
        self.subway.buildNoUserTripList()
        time_slot_average_walking_time = self.subway.filterTripListByWalkingTimeStation(target_station)

        # time_slot_average_walking_time.saveAsTextFile('/zf72/transportation_data/subway/output-walking-time/'+file_name+'/'+target_station+"_walking_time")

    def startTravelTimeFilteredByOriginDestination(self):
        start_station = "福田"
        end_station = "横岗"
        type_name = "evaluation_trips"
        file_name_prefix = 'P_GJGD_SZT_2016060'
        days = [6]
        for one_day in days:
            file_name = file_name_prefix + str(one_day)
            self.subway.setHDFSFilePath("/zf72/transportation_data/subway/input/"+file_name)
            self.subway.buildRecordList()
            self.subway.buildTripList()
            self.subway.buildNoUserTripList()
            self.subway.filterTripListByStartEndStation(start_station,end_station)
            self.subway.trip_list = self.subway.filtered_trip_list
            self.subway.buildTripStartTravelTime()
            self.subway.start_travel_time.saveAsTextFile('/zf72/transportation_data/subway/output/'+type_name+'/'+start_station+'_'+end_station+"/"+file_name)

    def statistics(self):
        # self.subway.setHDFSFilePath("/zf72/transportation_data/subway/input/*")
        self.subway.setHDFSFilePath("/zf72/transportation_data/sample/input/SZT_sample_0601.txt")
        self.subway.buildRecordList()
        self.subway.buildTripList()
        self.subway.buildNoUserTripList()
        num_records = self.subway.subway_record_list.count()
        num_users = self.subway.sorted_user_record_list.count()
        num_trips = self.subway.trip_list.count()
        print(num_records)
        print(num_users)
        print(num_trips)

    def startTimeVsNumberOfTrip(self):
        client = Config().get_client('dev')
        days = client.list(self.subway_file_dir)
        results = {}
        for day in days:
            try:
                day = [day]
                files = [self.subway_file_dir+one_file for one_file in day]
                file_path = ','.join(files)
                self.subway.setHDFSFilePath(file_path)
                count = self.subway.sc.parallelize((day[0],self.subway.input_data.count()))
                count.saveAsTextFile('/zf72/transportation_data/subway/off-peak-output/number-of-trips/'+day[0])
                results[day[0]] = self.subway.input_data.count()
            except:
                pass
        import json
        json.dump(results,open("../data_sync/trip-number.json",'w'))

    def averageTravelTime(self):
        client = Config().get_client('dev')
        days = client.list(self.subway_file_dir)
        results = {}
        for day in days:
            file_path = self.subway_file_dir + day
            self.subway.setHDFSFilePath(file_path)
            self.subway.setLocalFilePath('data_sync/sample-data/public_sample_old.txt')
            self.subway.buildRecordList()
        import json
        json.dump(results,open("../data_sync/average-travel-time.json",'w'))

    def numberOfStationVsDay(self):
        client = Config().get_client('dev')
        days = client.list(self.subway_file_dir)
        result = {}
        for day in days:
            file_path = self.subway_file_dir + day
            self.subway.setHDFSFilePath(file_path)
            self.subway.setLocalFilePath('../data_sync/sample-data/public_sample_old.txt')
            result[day] =  self.subway.buildStationNumber().count()
        import json
        json.dump(result,open("../data_sync/number-of-stations-changes.json",'w'))

    def passengerTravelTimeChangesOverTime(self):
        client = Config.get_client('dev')
        days = client.list(self.subway_file_dir)
        result = {}
        for day in days:
            file_path = self.subway_file_dir+ day
            self.subway.setHDFSFilePath(file_path)
            self.subway.setLocalFilePath('../data_sync/sample-data/public_sample_old.txt')
            self.subway.set


class BusMain():
    def __init__(self):
        self.bus = BusSpark()
    def maskBusID(self):
        # self.bus.setLocalFilePath("/home/zf72/Dropbox/projects/off-peak-trans/data/sample/bus_gps_sample_0603.txt")
        self.bus.setHDFSFilePath("/zf72/transportation_data/subway_bus/P_GJGD_SZT_20160601")
        self.bus.markSmartCardID(output_file_path="/zf72/transportation_data/result/mask/bus_data_0601")

    def saveBusTrip(self):
        # self.bus.setLocalFilePath("/media/zf72/Seagate Backup Plus Drive/E/DATA/SmartCityRawData/sz/sample/bus_gps_sample_0603.txt")
        file_name = 'STRING_20160601'
        self.bus.setHDFSFilePath("/zf72/transportation_data/bus/input/STRING_20160601")
        self.bus.buildRecordList()
        self.bus.buildTripList()
        self.bus.buildTripTimeGPSListForMapMatching()
        self.bus.trip_route_time_gps.saveAsTextFile('/zf72/transportation_data/bus/output/bus_trip_gps/'+file_name)

    def busWaitingTime(self):
        # self.bus.setLocalFilePath("/media/zf72/Seagate Backup Plus Drive/E/DATA/SmartCityRawData/sz/sample/bus_gps_sample_0603.txt")
        # file_name = 'STRING_20160601'
        self.bus.setHDFSFilePath("/zf72/transportation_data/bus/input/STRING_20160601")
        # 22.54598,114.046836
        start_station = [114.05157,22.539549]
        arrive_station = [114.05157,22.539549]
        # start_station = [114.053764,22.539549]
        # arrive_station = [114.053764,22.539549]
        self.bus.buildRecordList()
        # self.bus.filterByRouteName()
        self.bus.buildTripList()
        self.bus.buildPureTripList()
        self.bus.filterTripByGPSStation(start_station,arrive_station,filter_by= 'or')
        self.bus.buildRouteTripList()
        self.bus.start_arrive_time_list.saveAsTextFile('/zf72/transportation_data/bus/output/bus_start_end_time_2')

    def busWaitingTimeFilterByRoutes(self):
        # self.bus.setLocalFilePath("/media/zf72/Seagate Backup Plus Drive/E/DATA/SmartCityRawData/sz/sample/bus_gps_sample_0603.txt")
        self.bus.setHDFSFilePath("/zf72/transportation_data/bus/input/STRING_20160603")
        # 22.54598,114.046836
        start_station = [114.0537465,22.5395124]
        arrive_station = [114.0537465,22.5395124]

        #22.5395124,114.0537465
        self.bus.bus.range_gps = (0.1,0.1)
        self.bus.buildRecordList()
        self.bus.filterByRouteName()
        self.bus.buildTripList()
        self.bus.buildPureTripList()
        self.bus.filterTripByGPSStation(start_station,arrive_station,filter_by= 'or')
        self.bus.buildRouteTripList()
        self.bus.start_arrive_time_list.saveAsTextFile('/zf72/transportation_data/bus/output/bus_start_end_time_filter_routes')

    def busStationNames(self):
        self.bus.setHDFSFilePath("/zf72/transportation_data/bus/input/STRING_20160601")
        self.bus.buildRecordList()
        self.bus.buildBusRouteNames()
        self.bus.station_names.saveAsTextFile('/zf72/transportation_data/bus/output/bus_route_names')

    def statistics(self):
        # self.bus.setHDFSFilePath("/zf72/transportation_data/bus/input/*")
        self.bus.setHDFSFilePath("/zf72/transportation_data/subway/input/*")
        self.bus.buildSmartCardRecordList()
        self.bus.buildSmartCardUserTripList()
        # self.bus.buildRecordList()
        # self.bus.buildTripList()
        # self.bus.buildPureTripList()
        num_records = self.bus.smart_card_record_list.count()
        num_users = self.bus.user_record_list.count()
       # num_trips = self.bus.trip_list.count()
        print(num_records)
        print(num_users)
        #print(num_trips)
class TaxiMain():
    def __init__(self):
        self.taxi = TaxiSpark()
    def travelDelayTimeOneDayByHours(self):
        #self.taxi.setLocalFilePath("/media/zf72/Seagate Backup Plus Drive/E/DATA/SmartCityRawData/sz/sample/taxi_gps_sample_2016_06_01")
        # self.taxi.setHDFSFilePath('/zf72/transportation_data/taxi_gps/taxi_gps_sample_2016_06_01')
        self.taxi.setHDFSFilePath('/zf72/transportation_data/taxi_gps/GPS_2016_06_02')
        #self.taxi.setHDFSFilePath('')
        self.taxi.buildRecordList()
        self.taxi.buildTripList()
        self.taxi.buildODTravelTime()
        self.taxi.buildDelayTimeDistribution()
        self.taxi.average_delay_time.saveAsTextFile('/zf72/transportation_data/result/taxi_delay_time')

    def odTravelTime(self):
        self.taxi.setHDFSFilePath('/zf72/transportation_data/taxi_gps/GPS_2016_06_02')
        self.taxi.buildRecordList()
        self.taxi.buildTripList()
        self.taxi.buildODTravelTime()
        self.taxi.od_minimum_time.saveAsTextFile('/zf72/transportation_data/result/od_minimum_time_taxi')

    def travelTimeOneDayByHoursFilterByStartDistrict(self,district_name):
        self.taxi.setStartFilterDistrictName(district_name)
        self.taxi.setHDFSFilePath('/zf72/transportation_data/taxi_gps/GPS_2016_06_02')
        self.taxi.buildRecordList()
        self.taxi.buildTripList()
        self.taxi.buildODTravelTime()
        self.taxi.filterTripByStartDistrict(district_name)
        self.taxi.buildDelayTimeDistribution()
        self.taxi.average_delay_time.saveAsTextFile('/zf72/transportation_data/result/taxi_delay_time_districts/'+district_name)

    def travelTimeOneDayByHoursFilterByStartDistricts(self):
        districts = ['Luohu','Futian','Nanshan','Longgang','Baoan','Yantian']
        for one_district in districts:
            self.travelTimeOneDayByHoursFilterByStartDistrict(one_district)

    def travelTimeOneDayByHoursFilterByStartEndDistrict(self,start_district_name=None,end_district_name=None):
        self.taxi.setStartFilterDistrictName(start_district_name)
        self.taxi.setHDFSFilePath('/zf72/transportation_data/taxi/input/GPS_2016_06_01')
        self.taxi.buildRecordList()
        self.taxi.buildTripList()
        self.taxi.buildODTravelTime()
        if start_district_name is not None:
            self.taxi.filterTripByStartDistrict(start_district_name)
        if end_district_name is not None:
            self.taxi.filterTripByEndDistrict(end_district_name)
        self.taxi.buildDelayTimeDistribution()
        self.taxi.average_delay_time.saveAsTextFile('/zf72/transportation_data/taxi/output/taxi_delay_time_districts/'+start_district_name+"_"+end_district_name)

    def travelTimeOneDayByHoursFilterByStartEndDistricts(self):
        #districts = ['Luohu','Futian','Nanshan','Longgang','Baoan','Yantian']
        districts = ['Luohu','Futian']
        for one_district in districts:
            self.travelTimeOneDayByHoursFilterByStartEndDistrict(one_district,one_district)

    def test(self):
        pass

    def saveTrips(self):
        file_name = "taxi_2016_06_01"
        # self.taxi.setLocalFilePath("/media/zf72/Seagate Backup Plus Drive/E/DATA/SmartCityRawData/sz/sample/taxi_gps_sample_2016_06_01")
        self.taxi.test('/zf72/transportation_data/taxi/input/GPS_2016_06_01')
        # self.taxi.setHDFSFilePath('/zf72/transportation_data/taxi/input/GPS_2016_06_01')
        self.taxi.buildRecordList()
        self.taxi.buildTripList()
        self.taxi.buildTripTimeGPSListForMapMatching()
        self.taxi.trip_route_time_gps.saveAsTextFile('/zf72/transportation_data/taxi/output/taxi_trip_gps/'+file_name)

    def statistics(self):
        self.taxi.setHDFSFilePath("/zf72/transportation_data/subway/input/*/*")
        self.taxi.buildRecordList()
        self.taxi.buildTripList()
        self.taxi.buildNoUserTripList()
        num_records = self.taxi.subway_record_list.count()
        num_users = self.taxi.sorted_user_record_list.count()
        num_trips = self.taxi.trip_list.count()
        print(num_records)
        print(num_users)
        print(num_trips)
class PVMain():
    def __init__(self):
        self.pv = PVSpark()

    def travelDelayTimeOneDayByHours(self):
        #self.pv.setHDFSFilePath('/zf72/transportation_data/pv/input/20160101_000026.gtd.txt')
        self.pv.setHDFSFilePath('/zf72/transportation_data/pv/input/20160101_*.gtd.txt')
        # self.pv.setLocalFilePath("/media/zf72/Seagate Backup Plus Drive/E/DATA/SmartCityRawData/sz/privateCar/2016-01-txt/pv_sample_01_31")
        self.pv.buildRecordList()
        self.pv.buildTripList()
        self.pv.buildODTravelTime()
        self.pv.buildDelayTimeDistribution()
        self.pv.average_delay_time.saveAsTextFile('/zf72/transportation_data/pv/output/pv_delay_time')

    def travelTimeOneDayByHoursFilterByStartEndDistrict(self,start_district_name=None,end_district_name=None):
        self.pv.setStartFilterDistrictName(start_district_name)
        self.pv.setHDFSFilePath('/zf72/transportation_data/pv/input/20160101_*.gtd.txt')
        self.pv.buildRecordList()
        self.pv.buildTripList()
        self.pv.buildODTravelTime()
        if start_district_name is not None:
            self.pv.filterTripByStartDistrict(start_district_name)
        if end_district_name is not None:
            self.pv.filterTripByEndDistrict(end_district_name)
        self.pv.buildDelayTimeDistribution()
        self.pv.average_delay_time.saveAsTextFile('/zf72/transportation_data/result/pv_delay_time_districts/'+start_district_name+"_"+end_district_name)

    def travelTimeOneDayByHoursFilterByStartEndDistricts(self):
        #districts = ['Luohu','Futian','Nanshan','Longgang','Baoan','Yantian']
        districts = ['Luohu','Futian']
        for one_district in districts:
            self.travelTimeOneDayByHoursFilterByStartEndDistrict(one_district,one_district)

    def pvDemandDistribution(self):
        # self.pv.setLocalFilePath("/media/zf72/Seagate Backup Plus Drive/E/DATA/SmartCityRawData/sz/privateCar/2016-01-txt/20160107_131959.gtd.txt")
        self.pv.setHDFSFilePath('/zf72/transportation_data/pv/input/20160107_*.gtd.txt')
        self.pv.buildRecordList()
        self.pv.buildTripList()
        self.pv.demandDistributionOneDay()
        self.pv.time_slot_num.saveAsTextFile('/zf72/transportation_data/pv/output/time_slot_count/20160107/')

if __name__ == "__main__":
    # tran = BusMain()
    # tran.busWaitingTimeFilterByRoutes()
    # tran.saveBusTrip()
    # tran = BusMain()
    # tran.statistics()
    # tran.startTravelTimeFilteredByOriginDestination()
    # tran.riddingTimeMultipleDayBetweenTwoStations()
    # tran.travelTimeBetweenTwoStation()
    tran = SubwayMain()
    tran.walkingTimeEstimation()
    # tran.travelTimeBetweenTwoStation()
    # tran.travelTimeBetweenTwoStation()
    # tran.statistics()
    # tran.startTimeVsNumberOfTrip()
    # tran.travelTimeBetweenTwoStation()
    # tran.averageTravelTime()
    # tran.waitingTimeFilterByStartStation()
    # pv.travelDelayTimeOneDayByHours()
    # tran.waitingTimeInCity()
    # tran = TaxiMain()
    # tran.saveTrips()

