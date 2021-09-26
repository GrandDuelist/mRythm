# -*- coding: utf-8 -*-
class SubwayRouteRecord():
    def __init__(self,line_num=None,station_name=None,transfer=None,translate=None,order = None,one_line = None):
        self.line_num = line_num
        self.station_name = station_name
        self.transfer =transfer
        self.translate = translate
        self.one_line = one_line
        self.order =order
