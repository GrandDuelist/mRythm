# -*- coding: utf-8 -*-
from SparkInterval import *
from TravelTimeInterval import *
class TravelTimeSparkInterval(SparkInterval):
    def __init__(self,sc):
        self.sc = sc
        self.init()
        self.travel_time_interval = TravelTimeInterval()


    def averageDelayRatioOneDay(self):
        delay_ratio = self.input_data.map(self.travel_time_interval.parseTuple).filter(lambda x: x[0]!=x[1]).map(lambda x: (int(x[0]),int(x[1]),int(x[2]),float(x[3])))
        average_ratio_whole_day = delay_ratio.map(lambda x:((x[0],x[1]),x[3]))
        average_ratio_whole_day = self.buildAverageByKey(average_ratio_whole_day)
        return(average_ratio_whole_day)
    def averageDelayRatioOneDayByODPairs(self):
        # delay_ratio = self.input_data.map(self.travel_time_interval.parseTuple).filter(lambda x: float(x[3])<2.0).filter(lambda x: x[0]!=x[1]).map(lambda x: (x[0],x[1],int(x[2]),float(x[3])))
        delay_ratio = self.input_data.map(self.travel_time_interval.parseDelayRatioTuple).map(lambda x: (x[0],x[1],x[2],x[3],int(x[4]),float(x[5])))
        average_ratio_whole_day_temp = delay_ratio.map(lambda x:((x[0],x[1],x[2],x[3]),x[5]))
        self.od_pair_delay_one_day = self.buildAverageByKey(average_ratio_whole_day_temp)
        return(self.od_pair_delay_one_day)

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

    def odTimeDelayRatio(self):
        # trip_list_with_grid = self.trip_list.map(self.travel_time_interval.mapTripGPSToGrid)
        grid_slot_traveltime = self.trip_list.map(lambda trip: ((trip.start.grid,trip.end.grid,trip.timeSlot(t_hour=1)),trip.computeTripTimeToSeconds()))
        grid_slot_min = grid_slot_traveltime.reduceByKey(min)
        self.grid_slot_delay = grid_slot_traveltime.join(grid_slot_min).map(self.travel_time_interval.relativeDifferenceByKey)
    
    def tripGridStartTimeVsTravelTime(self):
        trip_station_start_time = self.trip_list.filter(lambda x: x is not None).map(lambda trip: (",".join([str(trip.start.grid), str(trip.end.grid), str(trip.timeSlot(t_min=60))]),trip.trip_time.total_seconds()))
        return(trip_station_start_time)
   
    def buildStatisticsByKey(self,key_values):
        def valuesToStatistics(key_values):
            (key,values) = key_values
            l = len(values); values.sort()
            one,two,three,four,five = None,None,None,None,None
            average = None; s = 0
            for ii in xrange(l):
                if ii == 0: one = values[0]
                if l/4 == ii: two = values[ii]
                if l/2 == ii: three = values[ii]
                if (3*l)/4 == ii: four= values[ii]
                if l-1 == ii: five = values[ii]
                s += values[ii]
            average = float(s)/float(l)
            return(key+","+",".join([str(one),str(two),str(three),str(four),str(five),str(average)]))
        key_values = key_values.groupByKey().mapValues(list)
        return(key_values.map(valuesToStatistics))
