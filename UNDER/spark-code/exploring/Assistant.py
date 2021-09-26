from datetime import datetime

class TimeAssistant():
    def parseTime(self,timeStr):
        return datetime.strptime(timeStr,'%Y-%m-%d %H:%M:%S')

    def parseTimeToStr(self,time_value):
        return(time_value.strftime("%Y-%m-%d %H:%M:%S"))
