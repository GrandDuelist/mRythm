 #coding=utf-8
from TravelTimeSpark import *
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
class TravelTimeSparkMain():
    def __init__(self):
        self.travel_time = TravelTimeSpark()
        self.input_dir = "/zf72/transportation_data/subway/input/"
        self.file_prefix = "P_GJGD_SZT_"
        self.output_dir = "/zf72/transportation_data/subway/off-peak-output/"
        self.local_output_dir = '../data_sync/subway/spark-output/'
        self.delay_ratio_input_dir = "/zf72/spark-results/subway/delay-ratio/"
        self.station_delay_ratio_input_dir = "/zf72/spark-results/subway/delay-ratio-station/"
        self.station_density_input_dir= "/zf72/spark-results/subway/passenger-station-density/"


    def startTimeVsNumberOfTrip(self):
        self.travel_time.startTimeVsNumberOfTrip(self.input_dir,self.output_dir,self.local_output_dir,self.file_prefix)

    def startTimeVsTravelTimeBetweenTwoStation(self):
        start_station = "罗湖站"
        end_station = "深圳北站"
        self.travel_time.travelTimeBetweenTwoStation(start_station,end_station,self.input_dir,self.output_dir,self.local_output_dir,self.file_prefix)
    def filterDelayRatioByStartEndStation(self):
        start_station = "罗湖站";end_station = "深圳北站"
        input_dir,output_dir = "/zf72/spark-results/subway/delay-ratio-station","/zf72/spark-results/subway/Luohu-Shenzhenbei-delay-ratio"
        self.travel_time.filterDelayRatioByStartEndStation(start_station,end_station,input_dir,output_dir,self.file_prefix)

    def dailyStationNumberMonth(self):
        self.travel_time.numberOfStations(self.input_dir, self.output_dir,self.local_output_dir,self.file_prefix)

    def mapRecordToODDelayRatio(self):
        self.travel_time.mapRecordToODDelayRatio()
    def mapRecordToODDelayRatioMonth(self):
        self.travel_time.mapRecordToODDelayRatioMonth(self.input_dir,self.file_prefix)
    def mapRecordToODDelayRatioStationToStation(self):
        self.travel_time.mapRecordToODDelayRatioStationToStation()
    def mapRecordToODDelayRatioMonthStationToStation(self):
        self.travel_time.mapRecordToODDelayRatioMonthStationToStation(self.input_dir,self.file_prefix)
    def averageDelayRatioOneMonth(self):
        self.travel_time.averageDelayRatioOneMonth()
    def averageDelayRatioMultiMonth(self):
        self.travel_time.averageDelayRatioMultiMonth(self.delay_ratio_input_dir,self.file_prefix,self.local_output_dir)
    def averageDelayRatioMultiMonthStation(self):
        self.travel_time.averageDelayRatioMultiMonthStation(self.station_delay_ratio_input_dir,self.file_prefix,self.local_output_dir)
    def averageDelayRatioHours(self):
        self.travel_time.averageDelayRatioByHours()
    def averageDelayRatioHoursMultiMonth(self):
        self.travel_time.averageDelayRatioByHoursMultiMonth(self.station_delay_ratio_input_dir,self.local_output_dir,self.file_prefix)
    def averageDelayRatioSingleUser(self):
        self.travel_time.averageDelayRatioSingleUser()
    def humanDensityInRegions(self):
        self.travel_time.passengerDensityInRegions()
    def humanDensityInRegionsMultiMonth(self):
        self.travel_time.passengerDensityInRegionsMultiMonths(self.input_dir,self.local_output_dir,self.file_prefix)
    def huamnDensityInRegions(self):
        self.travel_time.passengerDensityInStations()
    def humanDensityInStationMultiMonth(self):
        self.travel_time.passengerDensityInStationsMultiMonths(self.input_dir,self.local_output_dir,self.file_prefix)
    def humanDensityVsDelayRatioMultiMonthInStations(self):
        self.travel_time.passengerDensityDelayRatioInStationsMultiMonths(self.station_density_input_dir,self.station_delay_ratio_input_dir,self.local_output_dir,self.file_prefix)
        # self.travel_time.passengerDensityDelayRatioInStations(self.input_dir,self.local_output_dir,self.file_prefix)
    def testChineseCharactor(self):
        #self.travel_time.setHDFSFilePath(self.travel_time.public_sample)
        self.travel_time.setInputData(self.travel_time.sc.textFile(self.travel_time.public_sample,use_unicode=False))
        self.travel_time.input_data = self.travel_time.input_data.map(lambda x: str(x))
        self.travel_time.input_data.saveAsTextFile("/zf72/test-result")
        print(self.travel_time.input_data.collect())
    def tapOutVsDensityRatioMultiMonthInStation(self):
        self.travel_time.tapInTapOutVsDensityRatioMultiMonthInStation(self.station_density_input_dir,self.station_delay_ratio_input_dir,self.local_output_dir,self.file_prefix)
    def passengerTapOutInStationsMultiMonths(self):
        self.travel_time.passengerTapOutInStationsMultiMonths(self.input_dir,self.local_output_dir,self.file_prefix)


if __name__ == '__main__':
    travel_time = TravelTimeSparkMain()
    type=12
    if type==1:
        travel_time.startTimeVsNumberOfTrip()
    elif type == 2:
        travel_time.dailyStationNumberMonth()
    # travel_time.startTimeVsTravelTimeBetweenTwoStation()
    # travel_time.mapRecordToODDelayRatio()
    # travel_time.mapRecordToODDelayRatioMonthStationToStation()
    # travel_time.mapRecordToODDelayRatioMonth()
    # travel_time.averageDelayRatioMultiMonth()
    # travel_time.averageDelayRatioSingleUser()
    # travel_time.humanDensityInRegions()
    # travel_time.humanDensityInRegionsMultiMonth()
    # travel_time.averageDelayRatioMultiMonthStation()
    elif type == 9:
        travel_time.mapRecordToODDelayRatioMonth()
    elif type == 3:
        travel_time.averageDelayRatioHoursMultiMonth()
    elif type == 4:
        travel_time.humanDensityVsDelayRatioMultiMonthInStations()
    elif type == 5:
        travel_time.averageDelayRatioMultiMonth()
    elif type == 6:
        travel_time.humanDensityInRegionsMultiMonth()
    elif type == 7:
        travel_time.averageDelayRatioMultiMonthStation()
    elif type == 8:
        travel_time.humanDensityInStationMultiMonth()
    elif type == 11:
        travel_time.mapRecordToODDelayRatioMonthStationToStation()
    elif type ==10:
        travel_time.testChineseCharactor()
    elif type==12:
        travel_time.passengerTapOutInStationsMultiMonths()
