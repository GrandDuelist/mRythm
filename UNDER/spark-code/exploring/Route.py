#coding=utf-8
from Record import *

class RouteBean():
    def __init__(self,route_num=None,stations = None,station_names = None):
        self.route_num = route_num
        self.stations = stations
        self.station_names = station_names

class RouteHandler():
    def __init__(self):
        pass



class SubwayRouteHandler(RouteHandler):
    def __init__(self):
        self.routes = []
        self.existing_routes = {}
        self.station_line_mapping = None
        self.target_station = None
        self.target_previous_stations = None
        self.target_latter_stations = None
        self.target_pair_stations = None

    def parseOneLine(self,one_line):
        attrs = one_line.split(',')
        line_num = attrs[0]
        station_name = attrs[1]
        translate  = attrs[2]
        transfer = attrs[3]
        order = attrs[4]
        one_record = SubwayRouteRecord(line_num=line_num,station_name=station_name,translate=translate,transfer= transfer,order=order)
        return one_record

    def buildRoutes(self,file_path):
        with open(file_path) as file_handler:
            for one_line in file_handler:
                subwayRouteRecord = self.parseOneLine(one_line)
                if subwayRouteRecord.line_num in self.existing_routes.keys():
                    route_index = self.existing_routes[subwayRouteRecord.line_num]
                    self.routes[route_index].stations.append(subwayRouteRecord)
                    self.routes[route_index].station_names.append(subwayRouteRecord.station_name)
                else:
                    route_count = len(self.existing_routes.keys())
                    self.existing_routes[subwayRouteRecord.line_num] = route_count
                    route = RouteBean(route_num=subwayRouteRecord.line_num,stations=[subwayRouteRecord],station_names=[subwayRouteRecord.station_name])
                    self.routes.append(route)

    def buildStationLineMap(self,file_path):
        if self.station_line_mapping is not None:
            return self.station_line_mapping
        self.station_line_mapping = {}
        with open(file_path) as file_handler:
            for one_line in file_handler:
                subwayRouteRecord = self.parseOneLine(one_line)
                if subwayRouteRecord.station_name in self.station_line_mapping.keys():
                    self.station_line_mapping[subwayRouteRecord.station_name].append(subwayRouteRecord.line_num)
                else:
                    self.station_line_mapping[subwayRouteRecord.station_name] = [subwayRouteRecord.line_num]
        return(self.station_line_mapping)

    def findPreviousStationsInSameLine(self,station_name):
        target_lines = self.station_line_mapping[station_name]
        previous_stations = {}
        for one_line in target_lines:
            one_route = self.routes[self.existing_routes[one_line]]
            stations = one_route.station_names
            previous_stations[one_line] = stations[0:stations.index(station_name)]
        return(previous_stations)

    def findLatterStationsInSameLine(self,station_name):
        target_lines = self.station_line_mapping[station_name]
        latter_stations = {}
        for one_line in target_lines:
            one_route = self.routes[self.existing_routes[one_line]]
            stations = one_route.station_names
            latter_stations[one_line] = stations[stations.index(station_name)+1:]
        return(latter_stations)

    def findTripsAroundStationForWalkingTime(self,target_station):
        previous_stations_json = self.findPreviousStationsInSameLine(station_name=target_station)
        latter_stations_json = self.findLatterStationsInSameLine(station_name=target_station)
        all_key_pairs = {}
        all_pairs = []
        all_keys = previous_stations_json.keys()
        for one_key in all_keys:
            previous_stations = previous_stations_json[one_key]
            latter_stations = latter_stations_json[one_key]
            one_key_result = []
            for one_previous_station in previous_stations:
                for one_latter_station in latter_stations:
                    one_key_result.append([one_previous_station,one_latter_station])
                    all_pairs.append([one_previous_station,one_latter_station])
            all_key_pairs[one_key] = one_key_result
        return(all_key_pairs,all_pairs)

    def setTargetStation(self,target_station):
        self.target_station = target_station

    def buildTargetPreviousStations(self):
        self.target_previous_stations = self.findPreviousStationsInSameLine(self.target_station)

    def buildTargetLatterStations(self):
        self.target_latter_stations = self.findLatterStationsInSameLine(self.target_station)

    def buildTargetStationPairs(self):
        (keY_pairs,all_pairs) = self.findTripsAroundStationForWalkingTime(self.target_station)
        self.target_station_pairs = all_pairs

    def inPreviousStation(self,one_station):
        for one_key in self.target_previous_stations.keys():
            one_key_stations = self.target_previous_stations[one_key]
            if one_station in one_key_stations:
                return(True)
        return(False)

    def inLatterStation(self,one_station):
        for one_key in self.target_latter_stations.keys():
            one_key_stations = self.target_latter_stations[one_key]
            if one_station in one_key_stations:
                return(True)
        return(False)
    def tripInPreviousLatterSameLine(self,start_station,end_station):
        for one_key in self.target_previous_stations.keys():
            one_key_previous_stations = self.target_previous_stations[one_key]
            one_key_latter_stations = self.target_latter_stations[one_key]
            if start_station in one_key_previous_stations and end_station in one_key_latter_stations:
                return(True)
        return(False)

    def filterTripByPreviousStartAndTargetStation(self,one_trip):
        if self.inPreviousStation(one_trip.start.station_name) and one_trip.end.station_name == self.target_station:
            return(True)
        return(False)

    def filterTripByTargetStationAndPreviousStation(self,one_trip):
        if self.inPreviousStation(one_trip.end.station_name) and one_trip.start.station_name == self.target_station:
            return(True)
        return(False)

    def filterTripByTargetStationAndLatterStations(self,one_trip):
        if self.target_station == one_trip.start.station_name and self.inLatterStation(one_trip.end.station_name):
            return(True)
        return(False)

    def filterTripByLatterStationAndTargetStation(self,one_trip):
        if self.target_station == one_trip.end.station_name and self.inLatterStation(one_trip.start.station_name):
            return(True)
        return(False)

    def filterTripByPreviousLatterSameLine(self,one_trip):
        result = self.tripInPreviousLatterSameLine(start_station=one_trip.start.station_name,end_station=one_trip.end.station_name)
        if not result:
            result = self.tripInPreviousLatterSameLine(start_station=one_trip.end.station_name,end_station=one_trip.start.station_name)
        return(result)
#
# if __name__=='__main__':
#     subwayRoute= SubwayRouteHandler()
#     subwayRoute.buildRoutes("../data/shenzhen_subway_station_line.csv")
#     subwayRoute.buildStationLineMap("../data/shenzhen_subway_station_line.csv")
#     (all_key_pairs, all_pairs) = subwayRoute.getTripsAroundStationForWalkingTime("车公庙")
#     print(all_pairs)
