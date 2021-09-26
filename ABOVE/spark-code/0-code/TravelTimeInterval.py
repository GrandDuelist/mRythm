# -*- coding: utf-8 -*-
from Interval import *
class TravelTimeInterval(Interval):
    def __init__(self):
        self.init()

    def odTimeSlotDelayRatio(self,trip):
        return(self.interval.mapTripGPSToGrid(trip))

    def reduceByKeyMinimum(self,a,b):
        if a < b: return a
        else: return b

    def relativeDifferenceByKey(self,kv):
        (k,v) = kv
        return(k,(v[0] - v[1])/v[1])

    def parseDelayRatioTuple(self,row):
        attrs = row.split(',')
        return(attrs)

