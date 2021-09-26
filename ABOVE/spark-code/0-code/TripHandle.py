# -*- coding: utf-8 -*-
from TripEntity import *
class TripHandle():
    def mergeTwoTrip(self,first_trip,second_trip):
        start = first_trip.start
        end = second_trip.end
        route = []
        route.extend(first_trip.route)
        route.extend(second_trip.route)
        trip = Trip(start=start,end=end,start_time=start.time,end_time=end.time,route=route)
        return(trip)
    
    def parseRecordTotrip(self,record_list):
        (pv_id,record_list) = record_list
        n_records = len(record_list)
        all_trips = []
        route = []
        for ii in range(0,n_records):
            one_record = record_list[ii]
            route.append(one_record)
            if ii == n_records-1 or (record_list[ii+1].time - record_list[ii].time).total_seconds() > 60:
                start = route[0]
                end = route[-1]
                one_trip = Trip(start=start,end=end,route=route,start_time=start.time,end_time=end.time)
                one_trip.computeTripTime()
                one_trip.timeSlot(t_min=5)
                one_trip.arriveTimeSlot(t_min=5)
                if (one_trip.trip_time.total_seconds >30):
                    all_trips.append(one_trip)
                route = []
        new_all_trips = []
        for one_trip in all_trips:
            new_all_trips.extend(self.splitTripByTimeInterval(one_trip.route))
        return (pv_id,all_trips)

    def splitTripByTimeInterval(self,route):
        all_trip = []
        start_stop = route[0]
        end_start = route[0]
        is_end_start = False
        new_route = []
        n_records = len(route)
        for ii in range(0,n_records):
            one_stop = route[ii]

            if ii == n_records-1:
                end_start = one_stop
                new_route.append(one_stop)
                one_trip = Trip(start=start_stop,end=end_start,start_time=start_stop.time,end_time=end_start.time,route=new_route)
                all_trip.append(one_trip)
            else:
                next_stop = route[ii+1]
                if one_stop.lat == next_stop.lat and one_stop.lon == next_stop.lon:
                    if not is_end_start:
                        is_end_start = True
                        end_start = one_stop

                elif is_end_start:
                    one_trip = Trip(start=start_stop,end=end_start,route=new_route,start_time=start_stop.time,end_time=end_start.time)
                    start_stop = one_stop
                    is_end_start =False
                    all_trip.append(one_trip)
                    new_route = [start_stop]
                elif not is_end_start:
                    new_route.append(one_stop)

        new_all_trip = []
        n_trips =  len(all_trip)
        start_merge = False
        for ii in range(0,n_trips):
            if one_trip.isCircle():
                continue
            one_trip = all_trip[ii]
            if ii != n_trips-1:
                next_trip = all_trip[ii+1]
                if (next_trip.start.time - one_trip.end.time).total_seconds() < 20:  #if two trips interval is less than 1 min, merge two trips
                    if not start_merge:
                        new_trip = self.mergeTwoTrip(one_trip,next_trip)
                        start_merge = True
                    else:
                        new_trip = self.mergeTwoTrip(new_trip,next_trip)
                else:
                    if start_merge:
                        new_all_trip.append(new_trip)
                        start_merge = False
                        new_trip  = None
                    else:
                        new_all_trip.append(one_trip)
            else:
                if start_merge:
                    new_all_trip.append(new_trip)
                    start_merge = False

        return(new_all_trip)
