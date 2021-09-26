# -*- coding: utf-8 -*-
class TaxiODRecord():
    def __init__(self,plate=None,date=None,time = None,lon=None,lat = None, region=None, district =None,grid = None):
        self.plate = plate; self.date = date; self.time = time; self.lon = lon; self.lat = lat; self.region = region; self.district =district;
        self.grid = None
        
class TaxiRecord():
    def __init__(self,plate=None,date=None,time = None,lon=None,lat = None, region=None, district =None,grid=None):
        self.plate = plate; self.date = date; self.time = time; self.lon = lon; self.lat = lat; self.region = region; self.district =district;
        self.grid = None
    