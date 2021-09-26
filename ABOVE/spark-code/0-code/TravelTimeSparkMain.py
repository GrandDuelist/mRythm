# -*- coding: utf-8 -*-
from TravelTimeSpark import *
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
class TravelTimeSparkMain():
    def __init__(self,sc=None):
        self.travel_time = TravelTimeSpark()
        self.input_dir = "/zf72/transportation_data/taxi-od/input/"
        self.file_prefix = ""
        self.output_dir = "/zf72/transportation_data/taxi-od/output/"
        self.local_output_dir = '../data_sync/taxi-od/spark-output/'
        self.grid_delay_ratio_input_dir = "/zf72/transportation_data/taxi-od/output/delay-ratio-grid/"

    def startTimeVsNumberOfTripMultiMonthes(self):
        self.travel_time.startTimeNumberOfTripMultiMonths(self.input_dir,self.file_prefix,self.output_dir,self.local_output_dir)

    def startTimeVsNumberOfVehicleMultiMonthes(self):
        self.travel_time.startTimeVsNumberOfVehicleMultiMonthes(self.input_dir,self.file_prefix,self.output_dir,self.local_output_dir)

    def odTimeDelayRatioMultiMonthes(self):
        self.travel_time.odTimeDelayRatioMultiMonthes(self.input_dir,self.file_prefix,self.output_dir,self.local_output_dir)

    def averageDelayRatioMultiMonth(self):
        self.travel_time.averageDelayRatioMultiMonth(self.grid_delay_ratio_input_dir,self.output_dir,self.local_output_dir,self.file_prefix)

    def supplyMultiMonthes(self):
        self.supplyMultiMonthes(self.grid_delay_ratio_input_dir,self.output_dir,self.local_output_dir,self.file_prefix)
    
    def travelTimeBetweenLocations(self):
        self.travel_time.travelTimeBetweenLocations("/zf72/transportation_data/taxi-gps/0-four-month/0-input/","/zf72/transportation_data/taxi-gps/0-four-month/1-output/")
    def travelTimeBetweenLocationsOD(self):
        self.travel_time.travelTimeBetweenLocationsOD("/zf72/transportation_data/taxi-od/input-festival/","/zf72/transportation_data/taxi-od/output/1-delay-minmax-festival/")

traveltime = TravelTimeSparkMain()
#%% travel delay 
#traveltime.travelTimeBetweenLocations()
#%% travel delay between od
traveltime.travelTimeBetweenLocationsOD()
