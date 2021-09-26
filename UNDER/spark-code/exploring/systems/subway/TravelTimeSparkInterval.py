from SparkInterval import *
from TravelTimeInterval import *
class TravelTimeSparkInterval(SparkInterval):
    def __init__(self,sc):
        self.sc = sc
        self.init()
        self.travel_time_interval = TravelTimeInterval()


    def averageDelayRatioOneDay(self):
        delay_ratio = self.input_data.map(self.travel_time_interval.parseTuple).filter(lambda x: x[0]!=x[1]).map(lambda x: (int(x[0]),int(x[1]),int(x[2]),float(x[3])))
        # delay_ratio = self.input_data.map(lambda x: (tuple(x)))
        # print(delay_ratio.collect())
        average_ratio_whole_day = delay_ratio.map(lambda x:((x[0],x[1]),x[3]))
        average_ratio_whole_day = self.buildAverageByKey(average_ratio_whole_day)
        return(average_ratio_whole_day)
    def averageDelayRatioOneDayByStation(self):
        delay_ratio = self.input_data.map(self.travel_time_interval.parseTuple).filter(lambda x: float(x[3])<2.0).filter(lambda x: x[0]!=x[1]).map(lambda x: (x[0],x[1],int(x[2]),float(x[3])))
        # delay_ratio = self.input_data.map(lambda x: (tuple(x)))
        print(delay_ratio.count())
        average_ratio_whole_day_temp = delay_ratio.map(lambda x:((x[0],x[1]),x[3]))
        average_ratio_whole_day = self.buildAverageByKey(average_ratio_whole_day_temp)
        return(average_ratio_whole_day)
    def averageDelayRatioByHours(self):
        delay_ratio = self.input_data.map(self.subway.parseTuple).filter(lambda x: float(x[3])<1).map(lambda x: (x[0],x[1],int(x[2]),float(x[3])))
        average_ratio_hours_temp = delay_ratio.map(lambda x:(x[2],x[3]))
        average_ratio_hours = self.buildAverageByKey(average_ratio_hours_temp)
        return(average_ratio_hours)

    def tripListFilterByUser(self):
        trip_filtered_by_user = self.trip_list.filter(lambda x: x.start.user_id == "")
        self.trip_list = trip_filtered_by_user
        return(trip_filtered_by_user)

    def tapInDensityPerRegionPerTimeUnit(self):
        self.time_spatial_density = self.trip_list.map(lambda one_trip: ((one_trip.start.district,one_trip.timeSlot(t_min=5)),1)).reduceByKey(lambda x,y: x+y)
        self.time_spatial_density = self.time_spatial_density.map(lambda ((r,x,),d): (r,x,d))
        return(self.time_spatial_density)

    def tapInDensityPerStationPerTimeUnit(self):
        self.time_spatial_density = self.trip_list.map(lambda one_trip: ((one_trip.start.station_name,one_trip.timeSlot(t_min=5)),1)).reduceByKey(lambda x,y: x+y)
        self.time_spatial_density = self.time_spatial_density.map(lambda ((r,x,),d): (r,x,d))
        return(self.time_spatial_density)

    def tapOutPerStationPerTimeUnit(self):
        self.time_spatial_density = self.trip_list.map(lambda one_trip: ((one_trip.end.station_name,one_trip.arriveTimeSlot(t_min=5)),1)).reduceByKey(lambda x,y: x+y)
        self.time_spatial_density = self.time_spatial_density.map(lambda ((r,x,),d): (r,x,d))
        return(self.time_spatial_density)



