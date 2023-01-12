
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from env import *
from consts import *

import data_getter
from analyzer import Analyzer
from ticker.zigzag import Zigzag
from db.mysql import MySqlDB
import db.mydf as mydf
import lib
import lib.tradelib as tradelib
import lib.naming as naming

from sklearn.cluster import KMeans
import pandas as pd
import numpy as np
import json
import matplotlib.pyplot as plt
import math
import pickle
from datetime import datetime

zz_buff = 10
zz_min_km_count = 10
zz_min_item_count = 5
zz_min_confidence = 0.8
zz_min_lose_rate = 0.6
zz_max_k = 4

class ZzAnalyzer(Analyzer):
    def __init__(self, granularity, 
        n_points, kmpkl_file, 
        zz_size=5, 
        use_master=False,
        km_avg_size=ZZ_KM_AVG_SIZE):
        self.granularity = granularity
        self.n_points = n_points
        self.zz_size = zz_size
        self.kmpkl_file = kmpkl_file
        self.km_avg_size = km_avg_size
        self.kms = {}
        
        self.redb = MySqlDB(is_master=use_master)
        self.updb = MySqlDB(is_master=use_master)
        self.df = mydf.MyDf(is_master=use_master)
        

        dataTableName = naming.getZzDataTableName(granularity, zz_size)
        if self.redb.tableExists(dataTableName) == False:
            self.updb.createTable(dataTableName, "anal_zzdata")
        self.dataTableName = dataTableName


        codeTableName = naming.getZzCodeTableName(granularity)
        if self.redb.tableExists(codeTableName) == False:
            self.updb.createTable(codeTableName, "anal_zzcodes")
        self.codeTableName = codeTableName

        xys = ""
        stats_xys = ""
        xy = []
        stat_xy = []
        for i in range(n_points):
            if xys == "":
                xys = "x%d FLOAT, y%d FLOAT" % (i, i)
            else:
                xys += ",x%d FLOAT, y%d FLOAT" % (i, i)
            
            if i <= n_points-2:
                stat_xy.append(("x%d" % i, "y%d" % i))
            if i == n_points-2:
                stats_xys = xys
            xy.append(("x%d" % i, "y%d" % i))
        self.xy = xy
        self.stat_xy = stat_xy

        itemTableName = naming.getZzItemTableName(granularity, zz_size, n_points)
        if self.redb.tableExists(itemTableName) == False:
            self.updb.createTable(itemTableName, "anal_zzitems", {"#XYCOLUMS#": xys})
        self.itemTable = itemTableName


        statsTableName = naming.getZzKmStatsTableName(granularity, zz_size, n_points)
        if self.redb.tableExists(statsTableName) == False:
            self.updb.createTable(statsTableName, "anal_zzkmstats", {"#XYCOLUMS#": stats_xys})
        self.kmStatsTable = statsTableName

    
    def initZzCodeTable(self, year, codenames=[]):
        codeTableName = self.codeTableName
        #sql = "delete from %s where obsyear = %d" % (codeTableName, year)
        #self.updb.execSql(sql)
        granularity = self.granularity

        insql = "insert into %s(codename, obsyear, market, nbars, min_nth_volume) values" % (codeTableName)
        #is_first = True
        sql = "select codename, market from codes where (market = 'index' or industry33_code != '-')"
        sql += """ and codename not in 
(select codename from %s 
where obsyear = %d)""" % (codeTableName, year)
        if len(codenames) > 0:
            sql += " and codename in ('%s')" % "','".join(codenames)
        sql += ";"
        for (codename, market) in self.redb.execSql(sql):
            dg = data_getter.getDataGetter(codename, granularity)
            startep = lib.dt2epoch(datetime(year,1,1))
            endep = lib.dt2epoch(datetime(year,12,31))

            (_,_,_,_,_,_,v) = dg.getPrices(startep, endep, waitDownload=False)
            nbars = len(v)
            min_v = 0
            v.sort()
            if nbars > ZZ_N_MIN_VOLUMES*2:
                min_v = v[ZZ_N_MIN_VOLUMES-1]
            elif nbars > 0:
                min_v = v[0]
            
            self.updb.execSql(insql + "('%s',%d, '%s', %d, %d)" % (codename, 
                year, market, nbars, min_v))
            
        

    def getCodenamesFromDB(self, observe_year, market="", obsyear_cond="="):
        sql = """select distinct codename from 
%s where obsyear %s %d 
and min_nth_volume >= %d""" % (self.codeTableName, obsyear_cond, observe_year, ZZ_MIN_VOLUMES)
        if market != "":
            sql += " and market='%s'" % market
        sql += " order by codename;"
        codenames = []
        for (codename,) in self.redb.execSql(sql):
            codenames.append(codename)
        return codenames

    def getItemId(self, codename, startep):
        sql = """select zzitemid from %s
where
codename = '%s'
and startep = %d 
        """ % (self.itemTable,codename, startep)
        (itemid,) = self.redb.select1rec(sql)
        return itemid

    

    def _normalizeItem(self, ep, prices):
        st = ep[0]
        if len(prices) == self.n_points:
            ed = ep[-2]
            ma = max(prices[:-1])
            mi = min(prices[:-1])
        elif len(prices) == self.n_points-1:
            ed = ep[-1]
            ma = max(prices)
            mi = min(prices)
        else:
            return None

        lep = ed - st
        gap = ma - mi
        if gap == 0:
            return None

        vals = []
        for i in range(len(ep)):
            x = (ep[i]-st)/lep
            vals.append(x)
            y = (prices[i]-mi)/gap 
            vals.append(y)

        return vals

    """
    # prices must be normalized by self._normalizeItem
    def predictNext(self, ep, prices):
        if len(ep) != self.n_points-1 or len(prices) != self.n_points-1:
            return (-1,)*9

        vals = self._normalizeItem(ep, prices)
        if vals is None:
            return (-1,)*9
    """

    def predictNext(self, norm_vals, ep, prices):
        rootid = self._calcKmRootGroupId(norm_vals)
        kmid = 0
        if rootid in self.kms.keys():
            kmid = self.kms[rootid].predict([norm_vals])[0]
        groupid = self._getKmGroupId(rootid, kmid)
        is_first = True
        sqlcols = ""
        for i in range(len(ep)):
            if is_first == False:
                sqlcols += ", "
            else:
                is_first = False
            sqlcols += "x%d, y%d" % (i, i)
        sql = """select count, lose_count, meanx, meany, stdx, stdy, %s from %s
where km_groupid = '%s';""" % (sqlcols, self.kmStatsTable, groupid)

        res = self.redb.select1rec(sql)
        if res == None:
            return (-1,)*9
        (cnt, lose_cnt, meanx, meany, nstdx, nstdy) = res[:6]
        points = res[6:]

        xs = []
        ys = []
        for i in range(int(len(points)/2)):
            xs.append(points[2*i])
            ys.append(points[2*i+1])

        gapx = ep[-1] - ep[0]
        gapy = max(prices) - min(prices)

        (x, y, stdx, stdy) = ( 
            ep[-1] + gapx*meanx, 
            min(prices) + gapy*meany, 
            gapx*nstdx, 
            gapy*nstdy)
        return (cnt, lose_cnt, x, y, stdx, stdy, nstdx, nstdy, groupid)

    


    def _registerItem(self, codename, ep, prices, dirs, with_prediction=False):
        st = ep[0]
        ed = ep[-1]
        
        sql = """select count(*) from %s
where
codename = '%s'
and startep = %d 
        """ % (self.itemTable, codename, st)
        (cnt,) = self.redb.select1rec(sql)
        if cnt > 0:
            return self.getItemId(codename, st)

        vals = self._normalizeItem(ep, prices)
        if vals == None:
            return
        
        km_mode = ZZ_KMMODE_NONE
        km_groupid = ""
        if with_prediction:
            km_mode = ZZ_KMMODE_PREDICTED
            (_, _, _, _, _, _, _, _, km_groupid) = self.predictNext(vals[:-2], ep[:-1], prices[:-1])

        sqlcols = ""
        sqlvals = ""
        is_first = True
        for i in range(len(ep)):
            x = vals[2*i]
            y = vals[2*i+1]
            if is_first == False:
                sqlcols += ", "
                sqlvals += ", "
            else:
                is_first = False
            sqlcols += "x%d, y%d" % (i, i)
            sqlvals += "%f, %f" % (x, y)
            

        sql = """insert into %s(codename, startep, endep, km_groupid, km_mode, %s, last_dir) 
    values('%s', 
            %d, %d, 
            '%s', %d, %s, %d);
                """ % (self.itemTable, sqlcols, 
                        codename, 
                        st, ed, 
                        km_groupid, km_mode, sqlvals, dirs[-2])

        self.updb.execSql(sql)
        zzitemid = self.getItemId(codename, st)

        return zzitemid


    def registerData(self, startep, endep, codenames, with_prediction=False):
        granularity = self.granularity
        unitsecs = tradelib.getUnitSecs(granularity)
        buff_ep = unitsecs*zz_buff
        first = True

        if with_prediction:
            self.loadKmModel()

        for codename in codenames:
            log("Processing %s" % codename)

            sql = """select min(ep) startep, max(ep) endep from 
%s where codename = '%s';""" % (self.dataTableName, codename)
            res = self.redb.select1rec(sql)
            if res != None:
                (cstartep, cendep) = res
                if cstartep != None and cendep != None and (cstartep - buff_ep <= startep) and (cendep + buff_ep >= endep):
                    continue
                if cendep != None and endep > cendep:
                    startep = cendep + unitsecs

            dg = data_getter.getDataGetter(codename, granularity)
            try:
                if first:
                    ohlcv = dg.getPrices(startep, endep, waitDownload=False)
                    first = False
                else:
                    ohlcv = dg.getPrices(startep, endep, waitDownload=True)
            except Exception as e:
                log("codename=%s\n%s" % (codename, e))
            zz = Zigzag(codename, granularity, startep, endep)

            if len(zz.ep) < self.n_points:
                continue

            zz.initData(ohlcv, self.zz_size)    
            
            zz_ep = zz.zz_ep
            zz_dt = zz.zz_dt
            zz_prices = zz.zz_prices
            zz_dirs = zz.zz_dirs
            zz_dists = zz.zz_dists

            for i in range(len(zz_ep)):
                sql = """replace into 
%s(codename, EP, DT, P, dir, dist) 
values('%s', %d, '%s', %f, %d, %d);""" % (self.dataTableName, codename, 
                zz_ep[i], zz_dt[i], zz_prices[i], zz_dirs[i], zz_dists[i])
                self.updb.execSql(sql)

            n_points = self.n_points
            i = 0
            f_ep = []
            f_prices = []
            f_dirs = []
            len_zz_ep = len(zz_ep)
            while i < len_zz_ep-2:
                if abs(zz_dirs[i]) == 2:
                    f_ep.append(zz_ep[i])
                    f_prices.append(zz_prices[i])
                    f_dirs.append(zz_dirs[i])
                
                    if len(f_ep) == n_points -2:
                        j = i + 2
                        last_ep = 0
                        last_price = 0
                        last_dir = 0
                        while j<len_zz_ep:
                            if abs(zz_dirs[j]) == 2:
                                last_ep = zz_ep[j]
                                last_price = zz_prices[j]
                                last_dir = zz_dirs[j]
                                break
                            j += 1
                        if last_ep == 0:
                            break

                        self._registerItem(codename, 
                            f_ep + [zz_ep[i+1]] + [last_ep], 
                            f_prices + [zz_prices[i+1]] + [last_price], 
                            f_dirs + [zz_dirs[i+1]] + [last_dir], with_prediction=with_prediction)
                        f_ep.pop(0)
                        f_prices.pop(0)
                i +=1
        log("Zigzag registration completed!")

    def execKmeans(self, startep, endep, codenames):
        log("Starting kmeans calculation")
        '''
        df: index:  zzitemid
            values: x1,y1, x2,y2, ...
        '''
        sql = "select zzitemid"
        for i in range(self.n_points-1):
            sql += ", x%d, y%d" % (i, i)
        
        sql += " from %s where codename in ('%s')" % (self.itemTable, 
            "','".join(codenames))
        
        sql += " and startep >= %d and endep <= %d" % (startep, endep)

        
        df = pd.read_sql(sql, self.df.getConn())
        zzitemids = df["zzitemid"].tolist()
        del df["zzitemid"]

        log("Starting km_groupid calculation")

        #k = 3 ** (self.n_points-1)
        self._updateKmGroupId(zzitemids, df, "km_groupid")

        log("Completed!")


    def _calcKmRootGroupId(self, v):
        def get_b(d):
            b = "0"
            if abs(d) >= 0.01:
                if d > 0:
                    b = "1"
                else:
                    b = "2"
            return b

        n1 = self.n_points-3
        n2 = self.n_points-4
        g = [0]*(n1 + n2 + 1)
        for j in range(n1):
            l = j*2+1
            d = v[l+2*2]-v[l]
            g[j] = get_b(d)
        for j in range(n2):
            l = j*2+1
            d = v[l+3*2]-v[l]
            g[n1+j] = get_b(d)
        d = v[-1] - v[-3]
        g[-1] = get_b(d)
        gid = "".join(g)
        return gid

    def _getKmGroupId(self, rootid, kmid):
        return "%s-%s" % (rootid, str(kmid).zfill(10))

    def _getKmpklFilename(self, rootid):
        return "%s_%s" % (self.kmpkl_file, rootid)

    def _updateKmGroupId(self, zzitemids, df, column_name):
        root_groups = {}
        item_groups = {}
        vals = df.values
        for i in range(len(vals)):
            v = vals[i]
            gid = self._calcKmRootGroupId(v)
            if gid not in root_groups.keys():
                root_groups[gid] = []
                item_groups[gid] = []
            root_groups[gid].append(v)
            item_groups[gid].append(zzitemids[i])
            
        for gid in root_groups.keys():
            vals = root_groups[gid]
            k = min(int(len(vals)/self.km_avg_size), zz_max_k)
            if k > 1:
                km = KMeans(n_clusters=k)
                km = km.fit(vals)
                kmg = km.fit_predict(vals)
                # save kmeans model
                pklfile = self._getKmpklFilename(gid)
                with open(pklfile, "wb") as f:
                    pickle.dump(km, f)
            else:
                kmg = [0]*len(vals)
            
            item_group = item_groups[gid]
            for i in range(len(item_group)):
                sql = "update %s set %s = '%s', km_mode = %d where zzitemid = %d;" % (self.itemTable, 
                        column_name, self._getKmGroupId(gid, kmg[i]), ZZ_KMMODE_FEEDED, item_group[i])
                #print(sql)
                self.updb.execSql(sql)
            
        #self.km = km

    def loadKmModel(self):
        # to load
        sql = "SELECT distinct SUBSTRING_INDEX(km_groupid, '-', 1) as rootid from %s;" % (self.kmStatsTable)
        for (rootid,) in self.redb.execSql(sql):
            kmf = self._getKmpklFilename(rootid)
            if os.path.exists(kmf):
                with open(kmf, "rb") as f:
                    self.kms[rootid] = pickle.load(f)
            

    def calcClusterStats(self):
        mxy = ""
        uxy = ""
        cxy = ""
        for (x, y) in self.stat_xy:
            if mxy != "":
                mxy += ","
            mxy += "avg(%s) %s, avg(%s) %s" % (x, x, y, y)

            if uxy != "":
                uxy += ","
            uxy += "%s = values(%s), %s = values(%s)"  % (x, x, y, y)

            if cxy != "":
                cxy += ","
            cxy += "%s, %s" % (x, y)

        sql = """insert into 
%s(`km_groupid`, `count`, `lose_count`, `meanx`, `meany`, `stdx`, `stdy`, `last_epoch`, %s)         
(
    select item1.km_groupid, cnt, ifnull(lose_cnt,0), mx, my, sx, sy, last_epoch, %s
    from
    (
        select km_groupid, count(zzitemid) cnt, 
        avg(x%d) mx, avg(y%d) my, std(x%d) sx, std(y%d) sy,
        max(endep) last_epoch, %s  
        from %s
        where km_groupid is not null
        group by km_groupid
    ) item1
    left join
    (
        select km_groupid, count(zzitemid) lose_cnt from %s 
        where abs(last_dir) = 1
        group by km_groupid
    ) item2 on item1.km_groupid = item2.km_groupid

)
on duplicate key update 
`count` = values(`count`),
`lose_count` = values(`lose_count`),
`meanx` = values(`meanx`),
`meany` = values(`meany`),
`stdx` = values(`stdx`),
`stdy` = values(`stdy`),
`last_epoch` = values(`last_epoch`),
%s
;""" % (self.kmStatsTable, cxy,
        cxy, 
        self.n_points-1, self.n_points-1, self.n_points-1, self.n_points-1, 
        mxy, 
        self.itemTable, 
        self.itemTable, 
        uxy)
        self.updb.execSql(sql)
        



    def plotTopClusters(self, min_size=10):
        sql = """select km_groupid, 
count(km_groupid) km2cnt, avg(y%d) m, std(y%d) s  
FROM %s
where km_groupid is not null
group by km_groupid
having km2cnt >= %d
and s < 0.3
order by km2cnt desc
limit 12
;""" % (self.n_points-1, self.n_points-1,
        self.itemTable, 
        min_size)

        cls = {}
        for (grpid2, cnt, m, s) in self.redb.execSql(sql):
            cls[grpid2] = {}
            cls[grpid2]["count"] = cnt
            cls[grpid2]["m"] = m
            cls[grpid2]["s"] = s
            

        xs = []
        ys = []
        for i in range(self.n_points):
            xs.append("x%d" % i)
            ys.append("y%d" % i)

        ncol = 3
        nrow = math.ceil(len(cls)/ncol)
        csize = ncol*4
        rsize = nrow*4
        fig = plt.figure(figsize=(rsize, csize))

        i = 0
        for grpid2 in cls.keys():
            sql = """select codename, startep, endep, %s, %s from 
%s as item 
where km_groupid = '%s';""" % (",".join(xs), 
    ",".join(ys), 
    self.itemTable, grpid2)

            ax = fig.add_subplot(nrow, ncol, i+1)

            for tp in self.redb.execSql(sql):
                code = tp[0]
                startep = tp[1]
                endep = tp[2]
                x = np.asarray(tp[3:3+self.n_points])
                y = np.asarray(tp[3+self.n_points:3+2*self.n_points])
                legend = "%s %s-%s" % (code, 
                    lib.epoch2str(startep, "%Y-%m-%d"), 
                    lib.epoch2str(endep, "%Y-%m-%d"))

                ax.plot(x, y, label=legend)
                #ax.legend()
                ax.set_title("groupid=%s count=%d \nmean=%f std=%f" % (grpid2, 
                    cls[grpid2]["count"], cls[grpid2]["m"], cls[grpid2]["s"]))
            i += 1
    
    def getCorrOfPredicted(self):
        sql = "select max(count) from %s;" % (self.kmStatsTable)
        (max_cnt,) = self.redb.select1rec(sql)

        from db.mydf import MyDf
        db = MyDf()
        
        cnt = 10
        cnt_diff = 5
        min_kmgroups = 100
        i = 1
        rows = []
        while cnt < max_cnt:
            sql = """select i.km_groupid, abs(i.last_dir) d, 
k.lose_count/k.count lo
from %s as i
left join %s k on k.km_groupid = i.km_groupid
where i.km_mode = %d
and k.count >= %d and k.count < %d
;""" % (self.itemTable, self.kmStatsTable, ZZ_KMMODE_PREDICTED,
cnt, cnt+cnt_diff)
            df = db.read_sql(sql)
            #print(df.corr())
            #print("")
            corr_df = df.corr()
            if math.isnan(corr_df["d"]["lo"]) == False:
                sql = """select count(*) from %s 
where count >= %d and count < %d
;""" % (self.kmStatsTable, cnt, cnt+cnt_diff)
                (n_km_groups,) = self.redb.select1rec(sql)
                print("%d<=cnt<%d corr:%f n_km_groups:%d" % (cnt, 
                    cnt+cnt_diff, corr_df["d"]["lo"], n_km_groups))
                if n_km_groups >= min_kmgroups:
                    rows.append([cnt, float(corr_df["d"]["lo"])])
            i += 1
            cnt += 1

        df = pd.DataFrame(rows)
        print("corr of count and d-lo corr")
        print(df.corr())

        """
10<=cnt<15 corr:-0.078543 n_km_groups:17441
11<=cnt<16 corr:-0.067564 n_km_groups:15670
12<=cnt<17 corr:-0.055969 n_km_groups:13563
13<=cnt<18 corr:-0.050120 n_km_groups:11308
14<=cnt<19 corr:-0.047570 n_km_groups:9263
15<=cnt<20 corr:-0.044933 n_km_groups:7341
16<=cnt<21 corr:-0.042664 n_km_groups:5548
17<=cnt<22 corr:-0.044307 n_km_groups:4142
18<=cnt<23 corr:-0.044353 n_km_groups:3018
19<=cnt<24 corr:-0.043543 n_km_groups:2111
20<=cnt<25 corr:-0.039669 n_km_groups:1445
21<=cnt<26 corr:-0.025944 n_km_groups:988
22<=cnt<27 corr:-0.054118 n_km_groups:645
23<=cnt<28 corr:-0.071567 n_km_groups:421
24<=cnt<29 corr:-0.067095 n_km_groups:266
25<=cnt<30 corr:-0.051482 n_km_groups:189
26<=cnt<31 corr:-0.142642 n_km_groups:134
27<=cnt<32 corr:-0.186031 n_km_groups:89
28<=cnt<33 corr:-0.219131 n_km_groups:61
29<=cnt<34 corr:-0.231042 n_km_groups:44
30<=cnt<35 corr:-0.335766 n_km_groups:25
31<=cnt<36 corr:-0.174126 n_km_groups:13
32<=cnt<37 corr:-0.176675 n_km_groups:8
33<=cnt<38 corr:-0.295700 n_km_groups:4
34<=cnt<39 corr:-0.098513 n_km_groups:3
corr of count and d-lo corr
          0         1
0  1.000000 -0.695201
1 -0.695201  1.000000
        """

    
def _getAnalizer(jsonfile):
    import env
    data = ""
    with open("%s/%s" % (env.BASE_DIR, jsonfile), "r") as f:
        data = json.load(f)

    n_points = data["n_points"]
    granularity = data["granularity"]

    start = data["start_date"]
    end = data["end_date"]
    if len(start) == 10:
        start = start + "T00:00:00"
        end = end + "T00:00:00"

    startep = lib.str2epoch(start)
    endep = lib.str2epoch(end)
    home = os.environ["HOME"]
    work_dir = "%s/%s" % (home, data["work_dir"])        
    lib.ensureDir(work_dir)
    kmpkl_file = "%s/%s" % (work_dir, data["kmpkl_file"])
    a = ZzAnalyzer(granularity, n_points, kmpkl_file)

    codenames = []
    year = datetime.now().year
    if "observe_year" in data.keys():
        year = data["observe_year"]
    if "codenames" in data.keys():
        codenames = data["codenames"]
    else:
        codenames = a.getCodenamesFromDB(year)

    return (a, startep, endep, codenames, year)
    

def predict(jsonfile="anal_zz_predicts.json"):
    (a, startep, endep, codenames, _) = _getAnalizer(jsonfile)
    a.registerData(startep, endep, codenames, with_prediction=True)

def corr(jsonfile="anal_zz_predicts.json"):
    (a, _, _, _, _) = _getAnalizer(jsonfile)
    a.getCorrOfPredicted()
    

def feed(jsonfile="anal_zz_conf.json", stage="all"):
    (a, startep, endep, codenames, year) = _getAnalizer(jsonfile)

    if stage == "all" or stage == "init":
        a.initZzCodeTable(year, codenames)

    if stage == "all" or stage == "zigzag":
        a.registerData(startep, endep, codenames)

    if stage == "all" or stage == "kmeans":
        a.execKmeans(startep, endep, codenames=codenames)

    if stage == "all" or stage == "kmstats":
        a.calcClusterStats()

    if stage == "plot":
        a.plotTopClusters()

if __name__ == "__main__":
    #feed()
    #feed(stage="init")
    #feed(stage="zigzag")
    #feed(stage="kmeans")
    #feed(stage="kmstats")
    
    #predict()
    corr()