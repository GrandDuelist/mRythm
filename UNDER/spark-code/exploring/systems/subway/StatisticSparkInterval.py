from SparkInterval import *
class StatisticSparkInterval(SparkInterval):
    def __init__(self,sc):
        self.sc = sc
        self.init()
