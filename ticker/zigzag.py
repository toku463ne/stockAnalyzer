from ticker import Ticker
import lib.indicators as libind
from consts import *

class Zigzag(Ticker):
    def initData(self, ohlcv, size=5):
        self.updated = False
        self.size = size
        self.curr_zi = -1
        self.pos = 0
        (ep, dt, _, h, l, _, _) = ohlcv
        self.ep = ep
        self.tick_indexes = {}
        (self.zz_ep, 
        self.zz_dt, 
        self.zz_dirs, 
        self.zz_prices, _) = libind.zigzag(ep, dt, h, l, size)

        tick_indexes = []
        ti = 0
        ep = self.ep
        zz_ep = self.zz_ep
        for zi in range(len(zz_ep)):
            ze = zz_ep[zi]
            while True:
                if ze == ep[ti]:
                    tick_indexes.append(ti)
                    break
                ti += 1
        self.tick_indexes = tick_indexes


    def getData(self, i=-1, n=1):
        self.updated = False
        size = self.size
        tick_indexes = self.tick_indexes
        curr_zi = self.curr_zi

        if i == -1:
            i = curr_zi

        r = []
        if curr_zi == -1 or i+size <= tick_indexes[curr_zi]:
            r = range(len(tick_indexes))
        else:
            r = range(curr_zi, len(tick_indexes))
        
        for k in r:
            if i >= tick_indexes[k]+size:
                curr_zi = k
            else:
                break
        if curr_zi != self.curr_zi:
            self.updated = True
        self.curr_zi = curr_zi
        if curr_zi == -1:
            self.err = TICKER_NODATA
            return (0,None,0,0)

        if n == 1 and i >= 0:
            return (self.zz_ep[curr_zi], self.zz_dt[curr_zi], 
                self.zz_dirs[curr_zi], self.zz_prices[curr_zi])
        elif i >= 0:
            j = i-n+1
            if j < 0:
                j = 0
            i = i+1
            return (self.zz_ep[j:curr_zi+1], self.zz_dt[j:curr_zi+1], 
                self.zz_dirs[j:curr_zi+1], self.zz_prices[j:curr_zi+1])
        else:
            return (0,None,0,0)

