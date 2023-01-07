from data_getter.my_getter import MyGetter
from data_getter import DataGetter
import lib.naming as naming
import lib

class OnMemGetter(DataGetter):
    def __init__(self,childDG, tableNamePrefix="", is_dgtest=False, 
            memSize=10000, extendSize=100):
        self.name = "onmem_getter_%s_%s" % (childDG.codename, childDG.granularity)
        self.tableName = naming.priceTable(childDG.codename, childDG.granularity, tableNamePrefix)
        self.childDG = MyGetter(childDG, tableNamePrefix, is_dgtest=is_dgtest)
        self.unitsecs = self.childDG.unitsecs
        self.codename = self.childDG.codename
        self.granularity = self.childDG.granularity
        
        self.memSize = memSize
        self.extendSize = extendSize

        self.indexes = {} # {epoch: index}
        self.ep = []
        self.dt = []
        self.o = []
        self.h = []
        self.l = []
        self.c = []
        self.v = []

    def getPrice(self, epoch):
        i = self.getIndex(epoch)
        if i == -1:
            self.getPrices(epoch, epoch+1)
            i = self.getIndex(epoch)
            
        return (self.ep[i], self.dt[i], 
            self.o[i], self.h[i], 
            self.l[i], self.c[i], self.v[i])
        

    def getPrices(self, startep, endep, waitDownload=True):
        nowep = lib.nowepoch()
        endep = min(nowep, endep)

        starti = self.getIndex(startep)
        endi = self.getIndex(endep)
        if starti >= 0  and endi >= 0:
            return (self.ep[starti:endi+1], self.dt[starti:endi+1], 
            self.o[starti:endi+1], self.h[starti:endi+1], 
            self.l[starti:endi+1], self.c[starti:endi+1], self.v[starti:endi+1])

        
        if len(self.ep) == 0:
            if (endep - startep) / self.unitsecs < self.extendSize:
                    child_endep = min(startep + self.extendSize * self.unitsecs, nowep)
            else:
                child_endep = endep
            (ep, dt, o, h, l, c, v) = self.childDG.getPrices(startep, 
                    child_endep, waitDownload, buff_size=self.extendSize/2)
            self.ep = ep
            self.dt = dt
            self.o = o
            self.h = h
            self.l = l
            self.c = c
            self.v = v
            indexes = {}
            for i in range(len(ep)):
                indexes[ep[i]] = i
                self.indexes = indexes

            starti = self.getIndex(startep)
            endi = self.getIndex(endep)
            if starti <= 0 and endi == -1:
                return (ep, dt, o, h, l, c, v)
            else:
                return (ep[starti:endi+1], dt[starti:endi+1], 
                    o[starti:endi+1], h[starti:endi+1], 
                    l[starti:endi+1], c[starti:endi+1], v[starti:endi+1])
            
            
        child_startep = 0
        child_endep = 0
        if starti == -1:
            if startep < self.ep[0]:
                child_startep = startep
                child_endep = self.ep[0] - 1
                self._extendPrices(child_startep, child_endep, False, waitDownload)
            elif startep > self.ep[-1]:
                child_startep = self.ep[-1] + 1
                child_endep = endep
                self._extendPrices(child_startep, child_endep, True, waitDownload)
            else:
                ep = self.ep
                for i in range(len(ep)):
                    if startep > ep[i]:
                        startep = ep[i]
        if endi == -1:
            if endep > self.ep[-1]:
                if (endep - self.ep[-1]) / self.unitsecs < self.extendSize:
                    child_endep = self.ep[-1] + self.extendSize * self.unitsecs
                    child_endep = min(child_endep, nowep)
                else:
                    child_endep = endep
                child_startep = self.ep[-1]
                
                self._extendPrices(child_startep, child_endep, True, waitDownload)
            elif endep > self.ep[0]:
                for i in range(len(ep)):
                    if endep > ep[i]:
                        endep = ep[i]

        starti = self.getIndex(startep)
        endi = self.getIndex(endep)

        if len(self.ep) >= self.memSize + self.extendSize:
            deli = len(self.ep) - self.memSize
            if deli >= starti:
                deli = -1
                deli = starti

            
            if deli >= 1:
                self.ep = self.ep[deli:]
                self.dt = self.dt[deli:]
                self.o = self.o[deli:]
                self.h = self.h[deli:]
                self.l = self.l[deli:]
                self.c = self.c[deli:]
                self.v = self.v[deli:]
                self.resetIndex()
                starti = self.getIndex(startep)
                endi = self.getIndex(endep)
        return (self.ep[starti:endi+1], self.dt[starti:endi+1], 
            self.o[starti:endi+1], self.h[starti:endi+1], 
            self.l[starti:endi+1], self.c[starti:endi+1], self.v[starti:endi+1])

    def resetIndex(self):
        ep = self.ep
        indexes = {}
        for i in range(len(ep)):
            indexes[ep[i]] = i
        self.indexes = indexes

    def getIndex(self, epoch):
        if epoch in self.indexes.keys():
            return self.indexes[epoch]
        ep = self.ep
        if len(ep) == 0:
            return -1
        if ep[0] > epoch + self.unitsecs:
            return -1
        for i in range(len(ep)):
            if ep[i] > epoch:
                if i == 0:
                    return 0
                return i-1
        return -1

    def _extendPrices(self, child_startep, child_endep, extendAfter=True,waitDownload=True):
        (ep, dt, o, h, l, c, v) = self.childDG.getPrices(child_startep, 
                    child_endep, waitDownload, buff_size=self.extendSize)
        if extendAfter:
            self.ep.extend(ep)
            self.dt.extend(dt)
            self.o.extend(o)
            self.h.extend(h)
            self.l.extend(l)
            self.c.extend(c)
            self.v.extend(v)
        else:
            ep.extend(self.ep)
            dt.extend(self.dt)
            o.extend(self.o)
            h.extend(self.h)
            l.extend(self.l)
            c.extend(self.c)
            v.extend(self.v)
            
            self.ep = ep
            self.dt = dt
            self.o = o
            self.h = h
            self.l = l
            self.c = c
            self.v = v
        self.resetIndex()
            
        
    def drop(self):
        self.childDG.drop()



