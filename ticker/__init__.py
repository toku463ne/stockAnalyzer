import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import data_getter
from event.tick import TickEvent
from consts import *

class Ticker(object):
    def __init__(self, codename, granularity, 
        startep, endep=0, buffNbars=100, **args):
        self.codename = codename
        self.granularity = granularity
        self.buffNbars = buffNbars
        dg = data_getter.getDataGetter(self.codename, self.granularity)
        self.dg = dg
        self.unitsecs = dg.unitsecs
        self.args = args
        self._resetData(startep, endep)

    def _resetData(self, startep, endep=0):
        if endep == 0:
            endep = startep + self.unitsecs*self.buffNbars
        else:
            endep += self.unitsecs*self.buffNbars
        ohlcv = self.dg.getPrices(startep, endep)
        self.initData(ohlcv, **self.args)
        self.index = -1
        self.err = TICKER_ERR_NONE
        self.data = None


    # must inherit
    def initData(self, ohlcv, **args):
        (ep, dt, o, h, l, c, v) = ohlcv
        self.ep = ep
        self.dt = dt
        self.o = o
        self.h = h
        self.l = l
        self.c = c
        self.v = v

    # must inherit
    def getData(self, i=-1, n=1):
        if i == -1 and self.err == TICKER_ERR_NONE:
            i = self.index
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
        if self.tick():
            (ep, _, o, h, l, c, v) = self.data
            return TickEvent(ep, c, o, h, l, c, v)
        else:
            return None

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

    def getPrice(self, ep):
        if self.tick(ep) == False:
            if self.err == TICKER_ERR_EOF and self.index == -1:
                self._resetData(ep)
            if self.tick(ep) == False:
                raise Exception("No data for epoch=%d" % ep)
        return self.getData()  



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

    def _deleteOld(self, oldi):
        rmi = min(self.index, oldi)
        self.ep = self.ep[rmi+1:]
        self.dt = self.dt[rmi+1:]
        self.o = self.o[rmi+1:]
        self.h = self.h[rmi+1:]
        self.l = self.l[rmi+1:]
        self.c = self.c[rmi+1:]
        self.v = self.v[rmi+1:]
        self.index -= rmi

    def tick(self, ep=0):
        if self.err == TICKER_ERR_EOF:
            self._setCurrData(-1)
            self.err = TICKER_ERR_EOF
            return False
        n = len(self.ep)
        if n == 0:
            return False
        if ep > 0:
            if ep > self.ep[-1]:
                self.err = TICKER_ERR_EOF
                self._setCurrData(-1)
                self.index = -1
                return False
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
                        self.err = TICKER_NODATA
                        self._setCurrData(-1)
                    return True
                i += 1
        else:
            self.index += 1
            if self.index >= n:
                self.err = TICKER_ERR_EOF
                self._setCurrData(-1)
                return False
            i = self.index
            self._setCurrData(i)
            return True
        self.err = TICKER_NODATA
        self._setCurrData(-1)
        return False

