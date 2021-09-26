# -*- coding: utf-8 -*-
class MapFunctions():
    def statisticsByKey(self,key_values):
        (key,values) = key_values.groupByKey()
        l = len(values); values.sort()
        one,two,three,four,five = None,None,None,None,None
        average = None; s = 0
        for ii in xrange(l):
            if ii == 0: one = values[0]
            if l/4 == ii: two = values[ii]
            if l/2 == ii: three = values[ii]
            if (3*l)/4 == ii: four= values[ii]
            if l-1 == ii: five = values[ii]
            s += values[ii]
        average = float(s)/float(l)
        return([one,two,three,four,five,average])
                