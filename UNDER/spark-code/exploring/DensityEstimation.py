from __future__ import print_function
import os
from hdfs import Config
from hdfs import InsecureClient
import sys
from operator import add
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark import SparkContext
from DensityEstimationInterval import *


def TimeSlotDensity(sc):
    dir_path = "/zf72/human_mobility/China_Telecom"
    new_name = "density_time_slot_delete_useless"
    client = Config().get_client('dev')
    file_dates = client.list(dir_path)
    # file_dates = ['2013-10-22','2013-10-23','2013-10-24','2013-10-25','2013-10-26','2013-10-27','2013-10-28']
    # file_dates = ['2013-10-10']
    for file_date in file_dates:
        print(file_date)
        files = sc.textFile(dir_path+"/"+file_date+"/*").cache()
        lines = files.map(extract_user_slot_location_telecom).map(lambda (k,v):((v,k),1)).reduceByKey(lambda a,b: a+b)\
            .map(lambda ((k,userid),v):(k,1)).reduceByKey(lambda a,b: a+b).map(lambda (k,v): (tuple(k.split(',')),v))\
            .map(lambda (k,v): ((int(k[0]), float(k[1]), float(k[2])), v))\
            .sortByKey(True,keyfunc=lambda k: int(k[0]))
        lines.saveAsTextFile(dir_path+'_'+new_name+'/'+file_date)


if __name__ == "__main__":
    conf=SparkConf()
    conf.setMaster("spark://namenode:7077")
    conf.set("spark.executor.cores", "10")
    conf.setAppName("DensityEstimation")
    conf.set("spark.executor.memory", "64g")
    conf.set("spark.scheduler.mode", "FAIR")

    sc = SparkContext(conf=conf)
    sc.addFile("/home/zhihan/Dropbox/projects/off-peak-trans/src/exploring/DensityEstimationInterval.py")
    TimeSlotDensity(sc)
