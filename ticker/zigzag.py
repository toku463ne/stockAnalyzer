from ticker import Ticker
import lib.indicators as libind
import lib
from consts import *
from db.mydf import MyDf
import lib.naming as naming

class Zigzag(Ticker):
    def loadData(self, ohlcv, startep, endep, use_master=True, size=5, middle_size=2):
        (ep, _, _, _, _, _, _) = ohlcv
        self.ep = ep
        
        self._preinit(size, middle_size)
        db = MyDf(is_master=use_master)
        sql = """select ep, dt, dir, p, dist from %s
where codename='%s'  
and ep >= %d and ep <= %d""" % (naming.getZzDataTableName(self.granularity, self.size),
        self.codename,
        startep, endep)
        df = db.read_sql(sql)
        self.zz_ep = df["ep"].tolist()
        dt = []
        for dt64 in df.dt.values:
            dt.append(lib.npdt2dt(dt64))
        self.zz_dt = dt
        self.zz_dirs = df["dir"].tolist()
        self.zz_prices = df["p"].tolist()
        self.zz_dists = df["dist"].tolist()

        self._postinit(ep)

    def _preinit(self, size=5, middle_size=2):
        self.updated = False
        self.size = size
        self.middle_size = middle_size
        self.curr_zi = -1
        self.pos = 0
        self.tick_indexes = {}


    def _postinit(self, ep):
        tick_indexes = []
        ti = 0
        zz_ep = self.zz_ep
        for zi in range(len(zz_ep)):
            ze = zz_ep[zi]
            while True:
                if ze == ep[ti]:
                    tick_indexes.append(ti)
                    break
                ti += 1
        self.tick_indexes = tick_indexes

    def initData(self, ohlcv, size=5, middle_size=2):
        self._preinit(size, middle_size)
        
        (ep, dt, _, h, l, _, _) = ohlcv
        self.ep = ep
        (self.zz_ep, 
            self.zz_dt, 
            self.zz_dirs, 
            self.zz_prices, 
            self.zz_dists, 
            _) = libind.zigzag(ep, dt, h, l, size, middle_size=middle_size)

        self._postinit(ep)

    def getData(self, i=-1, n=1, zz_mode=ZZ_MODE_RETURN_MIDDLE):
        self.updated = False
        size = self.size
        middle_size = self.middle_size
        tick_indexes = self.tick_indexes
        curr_zi = self.curr_zi

        if i == -1 and curr_zi >= 0:
            i = tick_indexes[curr_zi]

        if i == -1:
            return (0,None,0,0,0)

        r = []
        if curr_zi == -1 or i+size <= tick_indexes[curr_zi]:
            r = range(len(tick_indexes))
        else:
            r = range(curr_zi, len(tick_indexes))
        
        for k in r:
            zzi = tick_indexes[k]
            if zz_mode == ZZ_MODE_RETURN_COMPLETED and i >= zzi+size:
                curr_zi = k
            elif (zz_mode == ZZ_MODE_RETURN_MIDDLE or \
                    zz_mode == ZZ_MODE_RETURN_ONLY_LAST_MIDDLE) \
                        and i >= zzi+middle_size:
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
            if zz_mode == ZZ_MODE_RETURN_MIDDLE:
                curr_zj = curr_zi-n+1
                if curr_zj < 0:
                    curr_zj = 0
                return (self.zz_ep[curr_zj:curr_zi+1], self.zz_dt[curr_zj:curr_zi+1], 
                        self.zz_dirs[curr_zj:curr_zi+1], self.zz_prices[curr_zj:curr_zi+1])
            else:
                ep = self.zz_ep
                dt = self.zz_dt
                drs = self.zz_dirs
                prices = self.zz_prices
                new_ep = [0]*n
                new_dt = [0]*n
                new_drs = [0]*n
                new_prices = [0]*n
                if zz_mode == ZZ_MODE_RETURN_ONLY_LAST_MIDDLE:
                    new_ep[-1] = ep[curr_zi]
                    new_dt[-1] = dt[curr_zi]
                    new_drs[-1] = drs[curr_zi]
                    new_prices[-1] = prices[curr_zi]
                curr_zj = curr_zi
                if zz_mode == ZZ_MODE_RETURN_COMPLETED:
                    while curr_zj >= 0:
                        if abs(drs[curr_zj]) == 2:
                            break
                        curr_zj -= 1
                    new_ep[-1] = ep[curr_zj]
                    new_dt[-1] = dt[curr_zj]
                    new_drs[-1] = drs[curr_zj]
                    new_prices[-1] = prices[curr_zj]

                curr_zj -= 1
                j = 2
                while curr_zj >= 0:
                    if abs(drs[curr_zj]) == 2:
                        new_ep[-j] = ep[curr_zj]
                        new_dt[-j] = dt[curr_zj]
                        new_drs[-j] = drs[curr_zj]
                        new_prices[-j] = prices[curr_zj]
                        j += 1
                        if j > n:
                            break
                    curr_zj -= 1
                return (new_ep[-j+1:],new_dt[-j+1:],new_drs[-j+1:],new_prices[-j+1:])

        else:
            return (0,None,0,0,0)

