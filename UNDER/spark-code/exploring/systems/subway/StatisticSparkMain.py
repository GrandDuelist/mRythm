from StatisticSpark import *
class StatisticSparkMain():
    def __init__(self):
        self.statistics = StatisticSpark()
        self.input_dir = "/zf72/transportation_data/subway/input/"
        self.file_prefix = "P_GJGD_SZT_"
        self.output_dir = "/zf72/transportation_data/subway/off-peak-output/"
        self.local_output_dir = '../data_sync/subway/spark-output/'
        self.delay_ratio_input_dir = "/zf72/spark-results/subway/delay-ratio/"
        self.station_delay_ratio_input_dir = "/zf72/spark-results/subway/delay-ratio-station/"
        self.station_density_input_dir= "/zf72/spark-results/subway/passenger-station-density/"
    def dailyNumberOfTripsMultiDays(self):
        self.statistics.dailyNumberOfTripMultiDays(self.input_dir,self.file_prefix,self.local_output_dir)
    def dailyNumberOfPassengers(self):
        self.statistics.dailyNumberOfPassengersMultiDays(self.input_dir,self.file_prefix,self.local_output_dir)

if __name__ == '__main__':
    statistics = StatisticSparkMain()
    type=2
    if type==1:
        statistics.dailyNumberOfTripsMultiDays()
    elif type==2:
        statistics.dailyNumberOfPassengers()
