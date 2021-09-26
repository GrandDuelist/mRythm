class Trip():
    def __init__(self,start=None,end=None,start_time=None,end_time=None,route= None):
        self.start = start
        self.end = end
        self.start_time = start_time
        self.end_time = end_time
        self.all_locations = None #used for intermediate points
        self.all_times = None #used for intermediate times
        self.in_vehicle_time = None
        self.trip_time = None
        self.waiting_time = None
        self.user_id = None
        self.route = route

    def setLocation(self,start,end):
        self.start = start
        self.end = end
    def setDistrictByStationName(self,subway):
        self.start.setDistrictByStationName(subway)
        self.end.setDistrictByStationName(subway)

    def setTime(self,start_time,end_time):
        self.start_time = start_time
        self.end_time = end_time
    def computeTripDistance(self):
        pass
    def computeTripTime(self):
        # print "start:" + str(self.start_time) + " end: " + str(self.end_time)
        self.trip_time = self.end_time - self.start_time
    def computeTripTimeToSeconds(self):
        return(self.trip_time.total_seconds())
    def setInvehicleTime(self,in_vehicle_time):
        self.in_vehicle_time = in_vehicle_time
    def computeWaitingTime(self):
        self.waiting_time = self.trip_time - self.in_vehicle_time
    def timeToMin(self,t_hour=None,t_min=None,t_sec=None):
        total_min = 0
        if t_hour is not None:
          total_min = total_min + t_hour * 60
        if t_min is not None:
            total_min = total_min + t_min
        if t_sec is not None:
            total_min = float(total_min) + float(t_sec)/float(60)
        return total_min

    def timeSlot(self,t_hour = None, t_min=None,t_sec = None):
        divide_min = self.timeToMin(t_hour,t_min,t_sec)
        start_min = self.timeToMin(self.start_time.hour,self.start_time.minute,self.start_time.second)
        slot = int(start_min/divide_min)
        self.timeslot = slot
        return slot


    def isCircle(self):
        return self.start.lon == self.end.lon and self.start.lat == self.end.lat

    def arriveTimeSlot(self,t_hour=None,t_min=None, t_sec =None):
        divide_min = self.timeToMin(t_hour,t_min,t_sec)
        end_min = self.timeToMin(self.end_time.hour,self.end_time.minute,self.end_time.second)
        slot = int(end_min/divide_min)
        self.end_timeslot = slot
        return slot

    def waitingTime(self,in_vehicle_time):
        # print in_vehicle_time
        self.waiting_time = self.trip_time - in_vehicle_time
        self.waiting_time_to_min = float(self.waiting_time.total_seconds())/float(60)
        return self.waiting_time_to_min
    def originDestination(self):
        if self.start.station_id is None or self.end.station_id is None:
            return (self.start.trans_region,self.end.trans_region)
        return (self.start.station_id,self.end.station_id)
