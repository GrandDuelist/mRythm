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
        self.subway_out_template = "/zf72/transportation_data/subway/output/{type_name}"
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

    def waitingTimeInAllStationsOneDay(self):
        outpath = self.subway_out_template.format(type_name='waiting-time-all-stations-0601')
        subway = self.subway
        subway.setHDFSFilePath('/zf72/transportation_data/subway/input/P_GJGD_SZT_20160601')
        # self.subway.setHDFSFilePath("/zf72/transportation_data/sample/input/SZT_sample_0601.txt")
        subway.buildRecordList()
        subway.buildTripList()
        subway.buildInVehicleTime()
        o_timeslot_waiting = subway.buildWaitingTimeStations("all_stations")
        o_timeslot_waiting.repartition(1).saveAsTextFile(outpath)

    def waitingTimeInAllStationMultipleDays(self):
        for ii in xrange(17,30):
            day = str(ii)
            if ii < 10: day = "0"+day
            outpath = self.subway_out_template.format(type_name='waiting-time-all-stations-06{day}'.format(day=day))
            subway = self.subway
            subway.setHDFSFilePath('/zf72/transportation_data/subway/input/P_GJGD_SZT_201606{day}'.format(day=day))
            # self.subway.setHDFSFilePath("/zf72/transportation_data/sample/input/SZT_sample_0601.txt")
            subway.buildRecordList()
            subway.buildTripList()
            subway.buildInVehicleTime()
            o_timeslot_waiting = subway.buildWaitingTimeStations("all_stations")
            o_timeslot_waiting.repartition(1).saveAsTextFile(outpath)

    def riddingTimeMultipleDays(self):
        for ii in xrange(1,31):
            day = str(ii)
            if ii < 10: day = "0"+day
            outpath = self.subway_out_template.format(type_name='ridding-time/all-trips-06{day}'.format(day=day))
            subway = self.subway
            subway.setHDFSFilePath('/zf72/transportation_data/subway/input/P_GJGD_SZT_201606{day}'.format(day=day))
            # self.subway.setHDFSFilePath("/zf72/transportation_data/sample/input/SZT_sample_0601.txt")
            subway.buildRecordList()
            subway.buildTripList()
            invehicleTime = subway.buildInVehicleTime()
            # print(invehicleTime.collect())
            output = invehicleTime.map(lambda x: ','.join([x[0][0],x[0][1],str(x[1])]))
            output.repartition(1).saveAsTextFile(outpath)

    def travelTimeMultipleDays(self):
        for ii in xrange(2,30):
            day = str(ii)
            if ii < 10: day = "0"+day
            outpath = self.subway_out_template.format(type_name='travel-time/hour-all-trips-06{day}'.format(day=day))
            subway = self.subway
            subway.setHDFSFilePath('/zf72/transportation_data/subway/input/P_GJGD_SZT_201606{day}'.format(day=day))
            # self.subway.setHDFSFilePath("/zf72/transportation_data/sample/input/SZT_sample_0601.txt")
            subway.buildRecordList()
            subway.buildTripList()
            subway.buildNoUserTripList()
            user_trip_time = self.subway.trip_list.map(lambda trip: ( ((trip.timeSlot(t_min=60),trip.start.station_name,trip.end.station_name),trip.trip_time.total_seconds())))
            user_trip_time_ave =user_trip_time.combineByKey(
                lambda v: (v,1),
                lambda x,v: (x[0]+v,x[1]+1),
                lambda x,y: (x[0]+y[0],x[1]+y[1])
            ).map(
                lambda (k,v): (k,float(v[0])/float(v[1]))
            )
            # print(user_trip_time_ave.collect())
            result = user_trip_time_ave.map(lambda (k,v): ','.join([str(k[0]),k[1],k[2],str(v)]))
            #invehicleTime = subway.buildInVehicleTime()
            # print(invehicleTime.collect())
            #output = invehicleTime.map(lambda x: ','.join([x[0][0],x[0][1],str(x[1])]))
            result.repartition(1).saveAsTextFile(outpath)



    def walkingTimeInStationsMultipleDays(self):
        pass

    def walkingTimeInStationOneDay(self):
        import pandas as pd
        data = pd.read_csv('data/station_name_201606.csv')
        stations = data['station name']
        for ii in xrange(len(stations)):
            station_name = stations.iloc[ii]
            self.walkingTimeEstimation(target_station=station_name,ii=1)

    def walkingTimeEstimation(self,target_station=None,ii=1):
        # if target_station is None:
        #     target_station = "横岗"
        day = str(ii)
        if ii < 10: day = "0" + day
        file_name = 'P_GJGD_SZT_201606{day}'.format(day=day)
        # self.subway.setHDFSFilePath("/zf72/transportation_data/sample/input/SZT_sample_0601.txt")
        self.subway.setHDFSFilePath("/zf72/transportation_data/subway/input/"+file_name)
        self.subway.buildRecordList()
        self.subway.buildTripList()
        self.subway.buildNoUserTripList()
        time_slot_average_walking_time = self.subway.filterTripListByWalkingTimeStation(target_station)
        time_slot_average_walking_time.repartition(1).saveAsTextFile('/zf72/transportation_data/subway/output-walking-time/'+file_name+'/'+target_station+"_walking_time")
if __name__ == '__main__':
    subway = SubwayMain()
    # subway.waitingTimeInAllStationsOneDay()
    # subway.waitingTimeInAllStationMultipleDays()
    # subway.walkingTimeInStationOneDay(
    subway.travelTimeMultipleDays()
