#from shapely.geometry import Polygon
#from scipy.spatial import distance
import json
#import pdb

class Grid():
    '''
    x y are gps 
    cor_x, cor_y are the grid number cordinate in two dimensions
    '''
    def __init__(self,start_x,start_y,step_x,step_y,cor_x,cor_y):
        self.start_x = start_x
        self.start_y = start_y
        self.step_x = step_x
        self.step_y = step_y
        self.end_x = start_x + step_x
        self.end_y = start_y + step_y
        self.cor_x = cor_x
        self.cor_y = cor_y
        self.gridCoordinate = (self.cor_x,self.cor_y)
        self.intersectTransRegion = [] 
    def set_corrdinates(self,cor_x,cor_y):
        self.cor_x = cor_x
        self.cor_y = cor_y
    def set_start_x_y(self,start_x,start_y):
        self.start_x = start_x
        self.start_y = start_y
    def isInGrid(self, test_x, test_y):
        if test_x >= self.start_x and test_x < self.end_x and test_y >= self.start_y and test_y < self.end_y:
            return True
        return False
    def getGridCoordinate(self):
        return self.gridCoordinate
    def addIntersectTransRegion(self,transRegion):
        if transRegion not in self.intersectTransRegion:
            self.intersectTransRegion.append(transRegion)
    def getIntersectTransRegion(self):
        return self.intersectTransRegion

class District():
    def __init__(self,vertices,district_id):
        self.vertices = vertices
        self.district_id = district_id

    def pnpoly(self,vertices,testx,testy):
        nvert = len(vertices)
        c = False
        j = nvert - 1
        for i in range(0,nvert):
            if ( ((vertices[i][1] > testy) != (vertices[j][1] > testy)) and (testx
                    <((vertices[j][0]-vertices[i][0])*(testy-vertices[i][1])/(vertices[j][1]-vertices[i][1]) +
                        vertices[i][0]))):
                        c = not c
            j = i
        return c

    def pointInDistrict(self,point):
        return self.pnpoly(self.vertices,point[0],point[1])
    def getGeoID(self):
        return self.district_id


class TransRegion():
    def __init__(self,vertices,region_id):
        self.vertices = vertices
        self.region_id = region_id
        # self.region_polygon = Polygon(self.vertices)
        self.grids = []
    def getRegionID(self):
        return self.region_id
    def addGrid(self,grid):
        if grid not in self.grids:
            self.grids.append(grid)
    def getGrids(self):
        return self.grids
    def getVertices(self):
        return self.vertices
    def pnpoly(self,vertices,testx,testy):
        nvert = len(vertices)
        c = False
        j = nvert - 1
        for i in range(0,nvert):
            if ( ((vertices[i][1] > testy) != (vertices[j][1] > testy)) and (testx
                    <((vertices[j][0]-vertices[i][0])*(testy-vertices[i][1])/(vertices[j][1]-vertices[i][1]) +
                        vertices[i][0]))):
                        c = not c
            j = i
        return c
    def pointInRegion(self,point):
        return self.pnpoly(self.vertices,point[0],point[1])

    def getGeoID(self):
        return self.region_id

class DistrictHandler():
    def __init__(self,minmax=None):
        self.districts = None
    def buildDistricts(self,out_edge):
        districts = []
        for one_region in out_edge:
            geo_array = one_region['geo_array']
            geo_id  = one_region['geo_id']
            one_region = District(geo_array,geo_id)
            districts.append(one_region)
        self.districts = districts
    def pointInRange(self,point):
        result =  point[0] >= self.min_x and point[0] <= self.max_x and point[1] >= self.min_y and point[1] <= self.max_y
        return result
    def findPointInDistrict(self,point):
        for one_district in self.districts:
            if one_district.pointInDistrict(point):
                return one_district
        return None
    def initDistritts(self,file_path):
        simple_gps = json.load(open(file_path))
        out_edge = simple_gps['out_edge']
        minmax = simple_gps['minmax']
        self.min_x = minmax['min_x']
        self.min_y = minmax['min_y']
        self.max_x = minmax['max_x']
        self.max_y = minmax['max_y']
        self.buildDistricts(out_edge)


class RegionHandler():
    def __init__(self,minmax=None):
        if minmax is not None:
            self.min_x = minmax['min_x']
            self.min_y = minmax['min_y']
            self.max_x = minmax['max_x']
            self.max_y = minmax['max_y']

    def buildGrids(self,grid_width,grid_height):
        self.grids = []
        self.grid_width = grid_width
        self.grid_height = grid_height
        self.x_grid_step = (self.max_x-self.min_x)/float(self.grid_width-1)
        self.y_grid_step = (self.max_y-self.min_y)/float(self.grid_height-1)
        self.grid_start_x  = self.min_x - self.x_grid_step/float(2)
        self.grid_start_y =  self.min_y - self.y_grid_step/float(2)
        for ii in range(0,self.grid_width):
            one_row = []
            current_start_x = self.grid_start_x +self.x_grid_step * ii
            for jj in range(0,self.grid_height):
                current_start_y  = self.grid_start_y + self.y_grid_step * jj
                one_grid = Grid(current_start_x,current_start_y,self.x_grid_step,self.y_grid_step,ii,jj) 
                one_row.append(one_grid)
            self.grids.append(one_row)
            
    def buildTransRegions(self,out_edge):
        # out_edge = simple_gps_json['out_edge']
        all_regions = []
        for one_region in out_edge:
            geo_array = one_region['geo_array']
            geo_id  = one_region['geo_id']
            one_region = TransRegion(geo_array,geo_id)
            all_regions.append(one_region)
        self.transRegions = all_regions
    def mapRegionToGrid(self):
        for one_region in  self.transRegions:
            vertices = one_region.getVertices()
            # for one_row in self.grids:
                # for one_grid in one_row:
            for vertice in vertices:
                one_grid = self.findPointGrid(vertice)
                # if one_grid.isInGrid(vertice[0],vertice[1]):
                one_grid.addIntersectTransRegion(one_region)
               # one_region.addGrid(one_grid)
                # print len(one_region.getGrids())


    def getTransRegions(self):
        return self.transRegions

    def regionCandiates(self,point):
        target_grid = self.findPointGrid(point)
        return target_grid.getIntersectTransRegion()

    def findPointGrid(self,point):
        x = point[0]
        y = point[1]
        grid_x = int((x -self.grid_start_x)/self.x_grid_step)
        grid_y = int((y -self.grid_start_y)/self.y_grid_step)
        target_grid = self.grids[grid_x][grid_y]
        # print target_grid.isInGrid(x,y)
        return target_grid

    def findPointInRegions(self,point,regions):
        for one_region in regions:
            if one_region.pointInRegion(point):
                return one_region
        return None

    def pointInRange(self,point):
        result =  point[0] >= self.min_x and point[0] <= self.max_x and point[1] >= self.min_y and point[1] <= self.max_y
        # if not result:
        #     print "INFO: point not in range"
        return result

    def findPointTransRegion(self,point):
        if not self.pointInRange(point):
            return None
        candidates = self.regionCandiates(point)
        #print "INFO: find point in candidate regions"
        #print "INFO: length of candidate ", len(candidates)
        result = self.findPointInRegions(point,candidates)
        if result is None:
           # print "INFO: find point in all regions"
            #print "INFO: length of transportation regions", len(self.transRegions)
            result = self.findPointInRegions(point,self.transRegions)
        return result

    def initializeGridRegion(self,file_path):
        simple_gps = json.load(open(file_path))
        out_edge = simple_gps['out_edge']
        minmax = simple_gps['minmax']
        self.min_x = minmax['min_x']
        self.min_y = minmax['min_y']
        self.max_x = minmax['max_x']
        self.max_y = minmax['max_y']
        self.buildGrids(50,50)
        self.buildTransRegions(out_edge)
        self.mapRegionToGrid()

    def initialize(self,grid_width,grid_height,out_edge):
        self.buildGrids(grid_width,grid_height)
        self.buildTransRegions(out_edge)
        self.mapRegionToGrid()
    
  
        

    # def isInTransRegion(self):
    #     pass

    # def isInTranRegion(self,polygons,polygon_ids,one_point):
    #     for ii in range(0,len(polygons)):
    #         one_polygon = polygons[ii]
    #         current_id = polygon_ids[ii]
    #         if one_polygon.contains(one_point):
    #             return current_id

    # def divideTransToGrids(self,simple_gps_json):
    #     minmax = simple_gps_json['minmax']
    #     self.minmax = minmax
    #     min_x = minmax['min_x']
    #     max_x = minmax['max_x']
    #     min_y = minmax['min_y']
    #     max_y = minmax['max_y']
    #     self.x_grid_step = (max_x-min_x)/float(self.grid_num_width)
    #     self.y_grid_step = (max_y-min_y)/float(self.grid_num_height)
    #     x_grid_starts = []
    #     y_grid_starts = []
    #     self.grids = []
    #     x_num = 0
    #     y_num = 0
    #     for ii in range(0,self.grid_num_width+1):
    #         current_start_x = min_x - self.x_grid_step/float(2) +self.x_grid_step * ii
    #         x_grid_starts.append(current_start_x)
    #         current_grid_row = []
    #         y_num = 0
    #         for jj in range(0,self.grid_num_height+1):
    #             current_start_y  = min_y - self.y_grid_step/float(2) + self.y_grid_step * jj
    #             current_grid_row.append(TransRegionGrid(current_start_x,current_start_y,self.x_grid_step,self.y_grid_step,x_num,y_num))
    #             y_grid_starts.append(current_start_y)
    #             y_num = y_num + 1
    #         x_num = x_num + 1
    #         self.grids.append(current_grid_row)
    #     self.x_grid_starts = x_grid_starts
    #     self.y_grid_starts = y_grid_starts

    # def assignTransToGrids(self,simple_gps_json):
    #     if self.x_grid_starts == None or self.y_grid_starts==None:
    #         self.divideTransToGrids(simple_gps_json)
    #     if self.regions == None or self.region_ids == None:
    #         self.buildPolygon(simple_gps_json)
    #     out_edge = simple_gps_json['out_edge']
    #     for one_region in out_edge:
    #         geo_array = one_region["geo_array"]
    #         geo_id = one_region["geo_id"]



    # def regionCandidates(self,polygons,polygon_ids,one_point):
    #     for ii in range(0,len(polygons)):
    #         one_polygon = polygons[ii]
    #         one_polygon_id = polygon_ids[ii]








