import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import data_getter
from event.tick import TickEvent
ERR_NONE = 0
ERR_NODATA = 1
ERR_EOF = 2

class Ticker(object):
    def __init__(self, codename, granularity, startep, endep, **args):
        self.codename = codename
        self.granularity = granularity
        dg = data_getter.getDataGetter(self.codename, self.granularity)
        ohlcv = dg.getPrices(startep, endep)
        self.unitsecs = dg.unitsecs
        self.initData(ohlcv, **args)
        self.index = -1
        self.err = ERR_NONE
        self.data = None

    # must inherit
    def initData(self, ohlcv, **args):
        (ep, dt, o, h, l, c, v) = ohlcv
        self.ep = ep.tolist()
        self.dt = dt
        self.o = o.tolist()
        self.h = h.tolist()
        self.l = l.tolist()
        self.c = c.tolist()
        self.v = v.tolist()

    # must inherit
    def getData(self, i, n=1):
        if n == 1 and i >= 0:
            return (self.ep[i], self.dt[i],self.o[i],self.h[i],self.l[i],self.c[i],self.v[i])
        elif i >= 0:
            j = i-n+1
            if j < 0:
                j = 0
            i = i+1
            return (self.ep[j:i], self.dt[j:i],self.o[j:i],
                self.h[j:i],self.l[j:i],self.c[j:i],
                self.v[j:i])
        else:
            return (0,None,0,0,0,0,0)

    def getTickEvent(self):
        self.tick()
        (ep, _, o, h, l, c, v) = self.data
        return TickEvent(ep, o, h, l, c, v)


    def getPrevIndex(self, ep, searchStartI=0):
        eps = self.ep
        for i in range(searchStartI, len(ep)):
            if ep < eps[i]:
                return i-1
            if ep == eps[i]:
                return i

    def getPrevEpoch(self, ep, searchStartI=0):
        i = self.getPrevIndex(ep, searchStartI)
        return self.ep[i]


    def getPostIndex(self, ep, searchStartI=0):
        eps = self.ep
        for i in range(searchStartI, len(ep)):
            if ep < eps[i]:
                return i
            if ep == eps[i]:
                return i


    def _setCurrData(self, i):
        if i >= 0:
            self.data = self.getData(i, 1)
        else:
            self.data = self.getData(-1)

    def tick(self, ep=0):
        if self.err == ERR_EOF:
            self._setCurrData(-1)
            self.err = ERR_EOF
            return False
        n = len(self.ep)
        if ep > 0:
            i = 0
            if self.index >= 0 and ep > self.ep[self.index]: 
                i = self.index
            while i < n:
                if ep == self.ep[i]:
                    self.index = i
                    self._setCurrData(i)
                    return True
                elif ep < self.ep[i]:
                    if i > 0:
                        j = i - 1
                        self.index = j
                        self._setCurrData(j)
                    else:
                        self.err = ERR_NODATA
                        self._setCurrData(-1)
                    return True
                i += 1
        else:
            self.index += 1
            if self.index >= n:
                self.err = ERR_EOF
                self._setCurrData(-1)
                return False
            i = self.index
            self._setCurrData(i)
            return True
        self.err = ERR_NODATA
        self._setCurrData(-1)
        return False

