from ModelSpark import *

class ModelSparkMain():
    def __init__(self):
        self.model_spark = ModelSpark()
        self.input_dir = "/zf72/transportation_data/subway/input/"
        self.file_prefix = "P_GJGD_SZT_"
        self.output_dir = "/zf72/transportation_data/subway/off-peak-output/"
        self.local_output_dir = '../data_sync/subway/spark-output/'
        self.delay_ratio_input_dir = "/zf72/spark-results/subway/delay-ratio/"
        self.station_delay_ratio_input_dir = "/zf72/spark-results/subway/delay-ratio-station/"
        self.station_density_input_dir= "/zf72/spark-results/subway/passenger-station-density/"

    def estimateNumberOfSubway(self):
        self.model_spark.estimateNumberOfSubway(self.delay_ratio_input_dir)

    def estimateSubwayCapacity(self):
        pass

if __name__ == '__main__':
    spark = ModelSparkMain()
    type=0
    if type==0:
        spark.estimateNumberOfSubway()
