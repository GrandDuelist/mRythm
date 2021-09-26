# -*- coding: utf-8 -*-
from __future__ import print_function
from pyspark import SparkConf
from pyspark import SparkContext
class Spark():
    def __init__(self):
        self.sc = None
        self.file_path = None
        self.input_data = None
    def connect(self):
        self.connectToSchool()
    
    def connectToSpyder(self,sc):
        self.sc =  sc
    
    def setLocalInputFile(self,inputFile):
        self.input_data = self.sc.parallelize(inputFile)

    def connectToSchool(self):
        # SparkContext.setSystemProperty("mapreduce.input.fileinputformat.input.dir.recursive",True)
        conf=SparkConf()
        conf.setMaster("spark://namenode:7077")
        conf.setAppName("off-peak")
        conf.set("spark.executor.memory", "50g")
        conf.set("spark.executor.cores", "5")
        conf.set("spark.cores.max","20")
        conf.set("spark.scheduler.mode", "FAIR")
        # conf.set("mapreduce.input.fileinputformat.input.dir.recursive", True)
        sc = SparkContext(conf=conf)
        self.sc =sc

    def setInputData(self,input_data):
        self.input_data = input_data
    def setLocalFilePath(self,file_path):
        self.file_path = file_path
        self.setLocalInputFile(open(self.file_path))
    def setHDFSFilePath(self,file_path):
        self.file_path = file_path
        self.setInputData(self.sc.textFile(self.file_path,use_unicode=False).cache())

