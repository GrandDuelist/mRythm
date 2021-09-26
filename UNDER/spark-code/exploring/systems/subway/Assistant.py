from datetime import datetime
# from hdfs import Config
import os
class TimeAssistant():
    def parseTime(self,timeStr):
        return datetime.strptime(timeStr,'%Y-%m-%d %H:%M:%S')

    def parseTimeToStr(self,time_value):
        return(time_value.strftime("%Y-%m-%d %H:%M:%S"))

    def getSparkFileInputString(self,file_path):
        pass
#
# class HDFSAssistant():
#     def __init__(self,env_name = 'dev'):
#         self.client = Config().get_client(env_name)
#
#     def isDirectory(self,file_path=None):
#         if file_path is None:
#             return(None)
#         return(self.client.status(file_path)['type']=="DIRECTORY")
#     def isFile(self,file_path=None):
#         if file_path is None:
#             return(None)
#         return(self.client.status(file_path)['type']=='FILE')
#     def createSparkInputString(self,file_path):
#         if self.isFile(file_path):
#             return(file_path)
#         if self.isDirectory(file_path):
#             files = []
#             for one_file in self.client.list(file_path):
#                 files.append(one_file)
#             return(",".join([os.path.join(file_path,one_file) for one_file in files]))


