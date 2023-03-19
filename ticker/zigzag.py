from ticker import Ticker
import lib.indicators as libind
import lib
from consts import *
from db.mydf import MyDf
import lib.naming as naming

class Zigzag(Ticker):
    def __init__(self, config):
        super(Zigzag, self).__init__(config)
        
        self.initAttrFromArgs(config, "size", 5)
        self.initAttrFromArgs(config, "middle_size", 2)
        self.curr_zi = -1
        self.last_i = -1
        self.pos = 0
        self.tick_indexes = []
        
        zzTableName = naming.getZigzagTableName(self.granularity, self.size, self.middle_size)
        self.ensureTable(zzTableName, "tick_zigzag")

        if self.startep > 0 or self.endep > 0:
            self.initData()
        
        

    def _loadData(self, ohlcv, startep, endep):
        use_master = self.use_master
        size = self.size
        middle_size = self.middle_size
        (ep, dt, o, h, l, c, v) = ohlcv
        self.ep = ep
        self.dt = dt
        self.o = o
        self.h = h
        self.l = l
        self.c = c
        self.v = v
        self.last_i = -1
        
        db = MyDf(is_master=use_master)
        sql = """select ep, dt, dir, p, v, dist from %s
where codename='%s'  
and ep >= %d and ep <= %d""" % (self.table,
        self.codename,
        startep, endep)
        df = db.read_sql(sql)
        zz_ep = df["ep"].tolist()
        self.zz_ep = zz_ep

        dt = []
        for dt64 in df.dt.values:
            dt.append(lib.npdt2dt(dt64))

        zz_dt = dt
        zz_dirs = df["dir"].tolist()
        zz_prices = df["p"].tolist()
        zz_v = df["v"].tolist()
        zz_dists = df["dist"].tolist()

        self.zz_dt = zz_dt 
        self.zz_dirs = zz_dirs 
        self.zz_prices = zz_prices 
        self.zz_v = zz_v
        self.zz_dists = zz_dists 

        return zz_ep, zz_dt, zz_prices, zz_v, zz_dirs, zz_dists


    def calcTickValues(self, ohlcv, size=5, middle_size=2):
        self.last_i = -1
        codename = self.codename
        (ep, dt, o, h, l, c, v) = ohlcv

        load_db = False
        startep = ep[0]
        endep = ep[-1]
        tableep_res = self.getTableEp()
        if tableep_res != None:
            (db_startep, db_endep) = tableep_res
            if startep > db_endep + self.unitsecs:
                raise Exception("Cannot save non continuous data to DB. startdt must be less than %s" % (lib.epoch2dt(db_endep+self.unitsecs)))
            
            if endep < db_startep - self.unitsecs:
                raise Exception("Cannot save non continuous data to DB. enddt must be more than %s" % (lib.epoch2dt(endep+selt.unitsecs)))
        
            # only load db when the range is inside what exist in DB for simplicity
            if startep >= db_startep and endep <= db_endep:
                load_db = True
        else:
            load_db = True


        self.ep = ep
        self.dt = dt
        self.o = o
        self.h = h
        self.l = l
        self.c = c
        self.v = v

        zz_ep = []
        zz_dt = []
        zz_prices = []
        zz_v = []
        zz_dirs = []
        zz_dists = []
        if load_db:
            zz_ep, zz_dt, zz_prices, zz_v, zz_dirs, zz_dists = self._loadData(ohlcv, startep, endep)


        if len(zz_ep) == 0:
            (zz_ep, 
            zz_dt, 
            zz_dirs, 
            zz_prices, 
            zz_v,
            zz_dists, 
            _) = libind.zigzag(ep, dt, h, l, v, size, middle_size=middle_size)

            self.zz_ep = zz_ep
            self.zz_dt = zz_dt 
            self.zz_dirs = zz_dirs 
            self.zz_prices = zz_prices 
            self.zz_v = zz_v
            self.zz_dists = zz_dists 
        

        if self.save_db and load_db == False:
            lenep = len(zz_ep)
            def insertDB(i, startep, endep):
                while i < lenep and zz_ep[i] < startep:
                    i += 1
                while i < lenep and zz_ep[i] <= endep:
                    sql = """replace into 
%s(codename, EP, DT, P, V, dir, dist) 
values('%s', %d, '%s', %f, %f, %d, %d);""" % (self.table, 
                    codename, zz_ep[i], zz_dt[i], zz_prices[i], zz_v[i], zz_dirs[i], zz_dists[i])
                    self.updb.execSql(sql)
                    i += 1
                return i

            i = -1
            if tableep_res == None:
                i = insertDB(0, startep, endep)
            else:
                if zz_ep[0] < db_startep:
                    i = insertDB(0, zz_ep[0], db_startep-self.unitsecs)
                    startep = zz_ep[0]
                if db_endep < zz_ep[-1]:
                    i = insertDB(i, db_endep+self.unitsecs, zz_ep[-1])
                    endep = zz_ep[-1]

            if i > 0: # there was an update
                self.updateTableEp(startep, endep)


        tick_indexes = []
        ti = 0
        for zi in range(len(zz_ep)):
            ze = zz_ep[zi]
            while True:
                #if ti >= len(ep):
                #    break
                if ze == ep[ti]:
                    tick_indexes.append(ti)
                    break
                ti += 1
        self.tick_indexes = tick_indexes


    def getCurrOhlcv(self):
        if self.last_i == -1:
            return (0,None,-1,-1,-1,-1,-1)
        
        i = self.last_i
        return (self.ep[i],self.dt[i],self.o[i],self.h[i],self.l[i],self.c[i],self.v[i])

    def getRecentOhlcv(self, n):
        if self.last_i == -1 or n <= 0:
            return ([],[],[],[],[],[],[])
        i = self.last_i
        j = i-n+1
        return (self.ep[j:i+1],self.dt[j:i+1],self.o[j:i+1],
                self.h[j:i+1],self.l[j:i+1],self.c[j:i+1],self.v[j:i+1])



    def getData(self, i=-1, n=1, zz_mode=ZZ_MODE_RETURN_ONLY_LAST_MIDDLE, do_update_flag_change=True):
        if do_update_flag_change:
            self.updated = False
        size = self.size
        middle_size = self.middle_size
        tick_indexes = self.tick_indexes
        curr_zi = self.curr_zi

        if i == -1 and curr_zi >= 0:
            i = tick_indexes[curr_zi]

        if i == -1:
            return (0,None,0,0,0)

        self.last_i = i

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
            if do_update_flag_change:
                self.updated = True
            self.curr_zi = curr_zi
        if curr_zi == -1:
            self.err = TICKER_NODATA
            return (0,None,0,0,0)

        ep = self.ep
        dt = self.dt
        h = self.h
        l = self.l
        v = self.v

        # True: must change the last peak
        # False: don't need to change
        def checkIfNeedChangeLastPeak():
            last_peak = 0
            last_dir = 0
            last_peak_i = -1
            if curr_zi < len(tick_indexes)-1:
                return False, last_peak_i, last_peak, last_dir
            if abs(self.zz_dirs[-1]) == 1:
                return False, last_peak_i, last_peak, last_dir
            if self.zz_dirs[-1] == 2:
                min_peak = 0
                min_j = -1
                for j in range(tick_indexes[curr_zi]+1, len(ep)):
                    if min_peak == 0 or l[j] < min_peak:
                        min_peak = l[j]
                        min_j = j
                if l[min_j] > min(l[min_j-size+1:min_j]):
                    return False, last_peak_i, last_peak, last_dir
                if min_j + self.middle_size < len(ep):
                    last_peak = min_peak
                    last_dir = -1
                    last_peak_i = min_j
                    return True, last_peak_i, last_peak, last_dir
            if self.zz_dirs[-1] == -2:
                max_peak = 0
                max_j = -1
                for j in range(tick_indexes[-1]+1, len(ep)):
                    if max_peak == 0 or h[j] > max_peak:
                        max_peak = h[j]
                        max_j = j
                if h[max_j] < max(h[max_j-size+1:max_j]):
                    return False, last_peak_i, last_peak, last_dir
                if max_j + self.middle_size < len(ep):
                    last_peak = max_peak
                    last_dir = 1
                    last_peak_i = max_j
                    return True, last_peak_i, last_peak, last_dir
            return False, last_peak_i, last_peak, last_dir

        if n == 1 and i >= 0:
            return (self.zz_ep[curr_zi], self.zz_dt[curr_zi], 
                    self.zz_dirs[curr_zi], self.zz_prices[curr_zi], self.zz_v[curr_zi])
            
        elif i >= 0:
            if zz_mode == ZZ_MODE_RETURN_MIDDLE:
                curr_zj = curr_zi-n+1
                if curr_zj < 0:
                    curr_zj = 0
                need_change, last_peak_i, last_peak, last_dir = checkIfNeedChangeLastPeak()
                if need_change:
                    curr_zj += 1
                    return (self.zz_ep[curr_zj:curr_zi+1] + [ep[last_peak_i]], 
                            self.zz_dt[curr_zj:curr_zi+1] + [dt[last_peak_i]], 
                            self.zz_dirs[curr_zj:curr_zi+1] + [last_dir], 
                            self.zz_prices[curr_zj:curr_zi+1] + [last_peak],
                            self.v[curr_zj:curr_zi+1] + [v[last_peak_i]])
                else:
                    return (self.zz_ep[curr_zj:curr_zi+1], self.zz_dt[curr_zj:curr_zi+1], 
                        self.zz_dirs[curr_zj:curr_zi+1], self.zz_prices[curr_zj:curr_zi+1],
                        self.zz_v[curr_zj:curr_zi+1])
            else:
                zz_ep = self.zz_ep
                zz_dt = self.zz_dt
                zz_drs = self.zz_dirs
                zz_prices = self.zz_prices
                zz_v = self.zz_v
                new_ep = [0]*n
                new_dt = [0]*n
                new_drs = [0]*n
                new_prices = [0]*n
                new_v = [0]*n
                if zz_mode == ZZ_MODE_RETURN_ONLY_LAST_MIDDLE:
                    new_ep[-1] = zz_ep[curr_zi]
                    new_dt[-1] = zz_dt[curr_zi]
                    new_drs[-1] = zz_drs[curr_zi]
                    new_prices[-1] = zz_prices[curr_zi]
                    new_v[-1] = zz_v[curr_zi]
                curr_zj = curr_zi
                if zz_mode == ZZ_MODE_RETURN_COMPLETED:
                    while curr_zj >= 0:
                        if abs(zz_drs[curr_zj]) == 2:
                            break
                        curr_zj -= 1
                    new_ep[-1] = zz_ep[curr_zj]
                    new_dt[-1] = zz_dt[curr_zj]
                    new_drs[-1] = zz_drs[curr_zj]
                    new_prices[-1] = zz_prices[curr_zj]
                    new_v[-1] = zz_v[curr_zj]

                curr_zj -= 1
                j = 2
                while curr_zj >= 0:
                    if abs(zz_drs[curr_zj]) == 2:
                        new_ep[-j] = zz_ep[curr_zj]
                        new_dt[-j] = zz_dt[curr_zj]
                        new_drs[-j] = zz_drs[curr_zj]
                        new_prices[-j] = zz_prices[curr_zj]
                        new_v[-j] = zz_v[curr_zj]
                        j += 1
                        if j > n:
                            break
                    curr_zj -= 1
                if zz_mode == ZZ_MODE_RETURN_ONLY_LAST_MIDDLE:
                    need_change, last_peak_i, last_peak, last_dir = checkIfNeedChangeLastPeak()
                    if need_change:
                        return (self.zz_ep[-j+2:] + [ep[last_peak_i]], 
                            self.zz_dt[-j+2:] + [dt[last_peak_i]], 
                            self.zz_dirs[-j+2:] + [last_dir], 
                            self.zz_prices[-j+2:] + [last_peak],
                            self.zz_v[-j+2:] + [v[last_peak_i]])
                return (new_ep[-j+1:],new_dt[-j+1:],new_drs[-j+1:],new_prices[-j+1:], new_v[-j+1:])

        else:
            return (0,None,0,0,0)

    def getLastMiddlePeak(self, last_i=-1):
        if last_i == -1:
            last_i = self.curr_zi
        eps = self.ep
        h = self.h
        l = self.l
        last_dir = self.zz_dirs[last_i]
        last_zz_i = self.tick_indexes[last_i]
        middle_size = self.middle_size
        if last_dir < 0:
            last_h = max(h[last_zz_i+1:last_zz_i+1+middle_size])
            for k in range(middle_size):
                if last_h == h[last_zz_i+1+k]:
                    middle_peak_ep = eps[last_zz_i+k+1]
                    middle_peak_price = last_h
                    break
                k += 1
        elif last_dir > 0:
            last_l = max(l[last_zz_i+1:last_zz_i+1+middle_size])
            for k in range(middle_size):
                if last_l == l[last_zz_i+1+k]:
                    middle_peak_ep = eps[last_zz_i+k+1]
                    middle_peak_price = last_l
                    break
                k += 1
        else:
            raise Exception("last_dir==0 Something wrong in zigzag generation!")
        last_ep = eps[last_zz_i+middle_size]
        last_price = self.c[last_zz_i+middle_size]
        return middle_peak_ep, middle_peak_price, last_ep, last_price
        