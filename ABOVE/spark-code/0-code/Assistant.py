# -*- coding: utf-8 -*-
from datetime import datetime
# from hdfs import Config
import os
class TimeAssistant():
    def parseTime(self,timeStr):
        return datetime.strptime(timeStr,'%Y-%m-%d %H:%M:%S')

    def parseTimeToStr(self,time_value):
        return(time_value.strftime("%Y-%m-%d %H:%M:%S"))

    def parseDate(self,dateStr):
        return datetime.strptime(dateStr,'%Y-%m-%d')

    def parseTimeWithouDate(self,timeStr):
        return(datetime.strptime(timeStr,'%H:%M:%S'))
