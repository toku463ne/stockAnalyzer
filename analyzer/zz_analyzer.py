
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

class ZzAnalyzer(Analyzer):
       
    def __init__(self, startep=0, endep=0, config={}, use_master=False):
        self.startep = startep
        self.endep = endep
        self.year = 0
        if endep > 0:
            self.year = lib.epoch2dt(endep).year
        self.initAttrFromArgs(config, "granularity", "D")
        self.initAttrFromArgs(config, "n_points", 5)
        self.initAttrFromArgs(config, "zz_size", 5)
        self.initAttrFromArgs(config, "km_granularity", "Y")
        self.initAttrFromArgs(config, "km_avg_size", 10)
        self.initAttrFromArgs(config, "km_max_k", 8)
        self.initAttrFromArgs(config, "codenames", [])

        data_dir = ""
        if "data_dir" in config.keys():
            data_dir = config["data_dir"]
        self.data_dir = lib.ensureDataDir(data_dir, subdir="zz")

        self.redb = MySqlDB(is_master=use_master)
        self.updb = MySqlDB(is_master=use_master)
        self.df = mydf.MyDf(is_master=use_master)
        self.kms = {}
        self.km_setid = ""
        
        granularity = self.granularity
        zz_size = self.zz_size
        n_points = self.n_points

        dataTableName = naming.getZzDataTableName(granularity, zz_size)
        self.ensureTable("dataTable", dataTableName, "anal_zzdata")

        codeTableName = naming.getZzCodeTableName(granularity)
        self.ensureTable("codeTable", codeTableName, "anal_zzcodes")

        kmGroupsTableName = naming.getZzKmGroupsTableName(granularity, zz_size, n_points)
        self.ensureTable("kmGroupsTable", kmGroupsTableName, "anal_zzkmgroups")

        kmGroupsPredictedTableName = naming.getZzKmGroupsTableName(granularity, zz_size, n_points)
        self.ensureTable("kmGroupsPredictedTable", kmGroupsPredictedTableName, "anal_zzkmgroups")

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
        self.ensureTable("itemTable", itemTableName, 
            "anal_zzitems", {"#XYCOLUMS#": xys})

        statsTableName = naming.getZzKmStatsTableName(granularity, zz_size, n_points)
        self.ensureTable("kmStatsTable", statsTableName, 
            "anal_zzkmstats", {"#XYCOLUMS#": stats_xys})

        self.codenames = self.getCodenamesFromDB()

    def dropAllTables(self):
        for tableName in [self.dataTable, self.codeTable, 
            self.kmGroupsTable, self.itemTable, self.kmStatsTable]:
            self.updb.dropTable(tableName)


    def ensureTable(self, table, tableName, tableTemplateName="", replacements={}):
        if self.redb.tableExists(tableName) == False:
            self.updb.createTable(tableName, tableTemplateName, replacements)
        setattr(self, table, tableName)

    
    def initZzCodeTable(self):
        year = self.year
        if year == 0:
            raise Exception("Need to init with startep end endep for initZzCodeTable")
        codenames = self.codenames
        codeTable = self.codeTable
        #sql = "delete from %s where obsyear = %d" % (codeTable, year)
        #self.updb.execSql(sql)
        granularity = self.granularity

        insql = "insert into %s(codename, obsyear, market, nbars, min_nth_volume) values" % (codeTable)
        #is_first = True
        sql = "select codename, market from codes where (market = 'index' or industry33_code != '-')"
        sql += """ and codename not in 
(select codename from %s 
where obsyear = %d)""" % (codeTable, year)
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

        self.codenames = self.getCodenamesFromDB()
            
    def getCodenamesFromDB(self, market=""):
        cnt = self.redb.countTable(self.codeTable, ["obsyear = %d" % (self.year)])
        if cnt == 0:
            return self.codenames

        sql = """select distinct codename from 
%s where obsyear = %d
and min_nth_volume >= %d""" % (self.codeTable, self.year ,
        ZZ_MIN_VOLUMES)

        if len(self.codenames) > 0:
            sql += " and codename in ('%s')" % "','".join(self.codenames)

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

    def devideNormalizedVals(self, vals):
        ep = []
        prices = []
        for i in range(self.n_points):
            ep.append(vals[2*i])
            prices.append(vals[2*i+1])
        
        return (ep, prices)


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

    def predictNext(self, ep, prices):
        norm_vals = self._normalizeItem(ep, prices)
        km_name = self._getKmName(norm_vals)
        kmid = 0
        km_groupid = 0
        if km_name in self.kms.keys():
            km_groupid = self.kms[km_name].predict([norm_vals])[0]
        km_id = naming.getKmId(km_name, km_groupid)
        
        sql = """select meanx, meany from %s
where km_id = '%s' and km_setid = '%s';""" % (self.kmStatsTable, km_id, self.km_setid)

        res = self.redb.select1rec(sql)
        if res == None:
            return (-1,-1, km_id)
        (meanx, meany) = res

        gapx = ep[-1] - ep[0]
        gapy = max(prices) - min(prices)

        x = gapx*meanx
        y = gapy*meany

        return (x, y, km_id)



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
        
        km_groupid = ""
        if with_prediction:
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
            

        sql = """insert into %s(codename, startep, endep, %s, last_dir) 
    values('%s', 
            %d, %d, 
            %s, %d);
                """ % (self.itemTable, sqlcols, 
                        codename, 
                        st, ed, 
                        sqlvals, dirs[-2])

        self.updb.execSql(sql)
        zzitemid = self.getItemId(codename, st)

        return zzitemid


    def registerData(self, with_prediction=False):
        startep = self.startep
        endep = self.endep
        if startep == 0 or endep == 0:
            raise Exception("Need to init with startep end endep for registerData")

        codenames = self.codenames
        if len(codenames) == 0 and len(self.codenames) > 0:
            codenames = self.codenames
        granularity = self.granularity
        unitsecs = tradelib.getUnitSecs(granularity)
        buff_ep = unitsecs*zz_buff
        first = True

        if with_prediction:
            self.loadKmModel()

        for codename in codenames:
            log("Processing %s" % codename)

            sql = """select min(ep) startep, max(ep) endep from 
%s where codename = '%s';""" % (self.dataTable, codename)
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
values('%s', %d, '%s', %f, %d, %d);""" % (self.dataTable, codename, 
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

    def execKmeans(self, km_setid=""):
        startep = self.startep
        endep = self.endep
        if startep == 0 or endep == 0:
            raise Exception("Need to init with startep end endep for execKmeans")
        codenames = self.codenames
        if km_setid == "":
            km_setid = naming.getZzKmSetId(startep, endep, codenames)
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
        self._updateKmGroupId(km_setid, zzitemids, df)

        log("Completed! km_setid=%s" % km_setid)
        self.km_setid = km_setid
        return km_setid


    def _getKmName(self, v):
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

    def _getKmpklFilename(self, km_setid, km_name):
        km_dir = "%s/%s" % (self.data_dir, km_setid)
        lib.ensureDir(km_dir)
        return "%s/%s.pkl" % (km_dir, km_name)

    def _updateKmGroupId(self, km_setid, zzitemids, df):
        km_groups = {}
        item_groups = {}
        vals = df.values
        for i in range(len(vals)):
            v = vals[i]
            km_name = self._getKmName(v)
            if km_name not in km_groups.keys():
                km_groups[km_name] = []
                item_groups[km_name] = []
            km_groups[km_name].append(v)
            item_groups[km_name].append(zzitemids[i])
            
        for km_name in km_groups.keys():
            vals = km_groups[km_name]
            k = min(int(len(vals)/self.km_avg_size), self.km_max_k)
            if k > 1:
                km = KMeans(n_clusters=k)
                km = km.fit(vals)
                kmg = km.fit_predict(vals)
                # save kmeans model
                pklfile = self._getKmpklFilename(km_setid, km_name)
                with open(pklfile, "wb") as f:
                    pickle.dump(km, f)
            else:
                kmg = [0]*len(vals)
            
            item_group = item_groups[km_name]
            for i in range(len(item_group)):
                #sql = "update %s set km_groupid = '%s', km_mode = %d where zzitemid = %d;" % (self.itemTable, 
                #        self._getKmGroupId(gid, kmg[i]), ZZ_KMMODE_FEEDED, item_group[i])
                #print(sql)

                km_id = naming.getKmId(km_name, kmg[i])
                sql = """insert into %s(zzitemid, km_id, km_setid, obsyear)  
values(%d, '%s', '%s', %d)
on duplicate key update 
`obsyear` = values(`obsyear`)
;""" % (self.kmGroupsTable,
                    item_group[i], km_id, km_setid, self.year)

                self.updb.execSql(sql)
            
        #self.km = km

    def loadKmModel(self, km_setid):
        # to load
        self.kms = {}
        sql = "select distinct km_id from %s where km_setid='%s';" % (self.kmStatsTable, km_setid)
        for (km_id,) in self.redb.execSql(sql):
            (km_name, km_groupid) = naming.extendKmId(km_id)
            kmf = self._getKmpklFilename(km_setid, km_name)
            if os.path.exists(kmf):
                with open(kmf, "rb") as f:
                    self.kms[km_name] = pickle.load(f)
        self.km_setid = km_setid
            
    def calcKmClusterStats(self, km_setid):
        mxy = ""
        uxy = ""
        cxy = ""
        for (x, y) in self.stat_xy:
            if mxy != "":
                mxy += ","
            mxy += "trimmean(%s,.2) %s, trimmean(%s,.2) %s" % (x, x, y, y)

            if uxy != "":
                uxy += ","
            uxy += "%s = values(%s), %s = values(%s)"  % (x, x, y, y)

            if cxy != "":
                cxy += ","
            cxy += "%s, %s" % (x, y)

        sql = """insert into 
%s(`km_id`, `km_setid`, `count`, 
`peak_count`, `meanx`, `meany`, `stdx`, `stdy`, `last_epoch`, 
%s)         
(
    select km_id, km_setid, cnt, dir_sum - cnt as peak_count, mx, my, sx, sy, last_epoch, %s
    from
    (
        select k.km_id, k.km_setid,  
        count(i.zzitemid) cnt, sum(abs(last_dir)) dir_sum,
        trimmean(x%d-x%d,.2) mx, trimmean(y%d-y%d, .2) my, 
        std(x%d-x%d) sx, std(y%d-y%d) sy,
        max(endep) last_epoch, %s
        from %s i
        left join %s k on k.zzitemid = i.zzitemid
        where k.km_id is not null and k.km_setid = '%s'
        group by k.km_id
    ) a
)
on duplicate key update 
`count` = values(`count`),
`peak_count` = values(`peak_count`),
`meanx` = values(`meanx`),
`meany` = values(`meany`),
`stdx` = values(`stdx`),
`stdy` = values(`stdy`),
`last_epoch` = values(`last_epoch`),
%s
;""" % (self.kmStatsTable, 
        cxy,
        cxy, 
        self.n_points-1, self.n_points-2, self.n_points-1, self.n_points-2, 
        self.n_points-1, self.n_points-2, self.n_points-1, self.n_points-2,
        mxy, 
        self.itemTable, 
        self.kmGroupsTable,
        km_setid,
        uxy
        )
        self.updb.execSql(sql)
        
     

    def getNFeededYears(self):
        sql = """select count(*) from (
select distinct year(FROM_UNIXTIME(startep)) year 
from %s g
left join %s i on i.zzitemid = g.zzitemid
where g.km_setid = '%s') a""" % (self.kmGroupsTable, self.itemTable, self.km_setid)
        (ycnt,) = self.redb.select1rec(sql)
        return ycnt

    """
    Deflection score is 
    """
    def getDeflectedKmGroups(self, deflect_rate=0.65, min_cnt_a_year=10):
        sql = """SELECT g.km_id, year(FROM_UNIXTIME(i.startep)) year,
abs(i.last_dir) dir, 
count(abs(i.last_dir)) cnt
FROM %s g
left join %s i on i.zzitemid = g.zzitemid
where g.km_setid = '%s'
group by g.km_id, year, dir
having cnt >= %d
""" % (self.kmGroupsTable, self.itemTable, self.km_setid, min_cnt_a_year)

        df = self.df.read_sql(sql)
        df1 = df[df["dir"] == 1]
        df2 = df[df["dir"] == 2]
        dfm = pd.merge(df1[["km_id","year", "cnt"]], 
                df2[["km_id","year", "cnt"]], 
                on=["km_id", "year"], how="outer").replace(np.nan, 0)
        dfm["total"] = dfm["cnt_x"]+dfm["cnt_y"]
        dfm["r1"]  = dfm["cnt_x"]/dfm["total"]

        dftotal = dfm.groupby(["km_id"]).count()[["total"]]
        df1 = dfm[dfm["r1"] >= deflect_rate].groupby(["km_id"]).count()[["total"]]
        df2 = dfm[dfm["r1"] <= 1-deflect_rate].groupby(["km_id"]).count()[["total"]]
        df1 = df1.rename(columns={"total": "total1"})
        df2 = df2.rename(columns={"total": "total2"})

        df = pd.merge(dftotal, df1, on=["km_id"], how="outer").replace(np.nan, 0)
        df = pd.merge(df, df2, on=["km_id"], how="outer").replace(np.nan, 0)
        df["score1"] = df["total1"]/df["total"]
        df["score2"] = df["total2"]/df["total"]

        return df



    def plotKmGroups(self, km_groupids, vsize=5, hsize=15):
        ncol = 1
        nrow = math.ceil(len(km_groupids)/ncol)
        fig = plt.figure(figsize=[hsize,vsize*nrow])
        xs = []
        ys = []
        for i in range(self.n_points):
            xs.append("x%d" % i)
            ys.append("y%d" % i)

        i = 0
        for km_groupid in km_groupids:
            sql = """select item.codename, item.startep, item.endep, 
meanx, meany, stdx, stdy,
%s, %s from 
%s as item 
left join 
(select km_groupid, meanx, meany, stdx, stdy from %s) ks 
on item.km_groupid = ks.km_groupid
where item.km_groupid = '%s';""" % (",".join(xs), 
            ",".join(ys), 
            self.itemTable, self.kmStatsTable, km_groupid)
            #print(sql)

            ax = fig.add_subplot(nrow, ncol, i+1)

            for tp in self.redb.execSql(sql):
                code = tp[0]
                startep = tp[1]
                endep = tp[2]
                meanx = tp[3]
                meany = tp[4]
                stdx = tp[5]
                stdy = tp[6]
                x = np.asarray(tp[7:7+self.n_points])
                y = np.asarray(tp[7+self.n_points:7+2*self.n_points])

                if x[-1] > meanx+3*stdx or x[-1] < meanx-3*stdx:
                    continue
                if y[-1] > meany+3*stdy or y[-1] < meany-3*stdy:
                    continue

                legend = "%s %s-%s" % (code, 
                    lib.epoch2str(startep, "%Y-%m-%d"), 
                    lib.epoch2str(endep, "%Y-%m-%d"))

                ax.plot(x, y, label=legend)
                #ax.legend()
                ax.set_title("groupid=%s" % (km_groupid))
            i += 1

def getValue(key, default=None):
    if key in config:
        return config[key]
    elif default is None:
        raise Exception("%s is necessary" % (key))
    else:
        return default

def plotKms(km_groupids, jsonfile="anal_zz_conf.json", vsize=5, hsize=15):
    data = ""
    with open("%s/%s" % (BASE_DIR, jsonfile), "r") as f:
        data = json.load(f)
    n_points = data["n_points"]
    granularity = data["granularity"]
    kmpkl_file = ""
    a = ZzAnalyzer(granularity, n_points, kmpkl_file)
    a.plotKmGroups(km_groupids, vsize, hsize)

def predict(config):
    ZzAnalyzer(config).registerData(startep, endep, codenames, with_prediction=True)


def feed(startep, endep, config={}, stage="all", km_setid=""):
    a = ZzAnalyzer(startep, endep, config=config, use_master=False)

    if stage == "init" or stage == "all":
        a.initZzCodeTable()

    if stage == "register" or stage == "all":
        a.registerData()

    if stage == "kmeans" or stage == "all":
        km_setid = a.execKmeans()

    if stage == "kmstat" or stage == "all":
        a.calcKmClusterStats(km_setid)
        

    

def strStEd2ep(start, end):
    if len(start) == 10:
        start = start + "T00:00:00"
        end = end + "T00:00:00"

    startep = lib.str2epoch(start)
    endep = lib.str2epoch(end)

    return startep, endep

if __name__ == "__main__":
    import sys
    start = "2010-01-01"
    end = "2020-12-31"
    (startep, endep) = strStEd2ep(start, end)
    config = {
        "granularity": "D",
        "n_points": 5,
        "zz_size": 5,
        "data_dir": "app/stockAnalyzer/test",
        "km_granularity": "Y",
        "codenames": []
    }

    #feed()
    #feed(stage="init")
    #feed(stage="zigzag")
    #feed(startep, endep, stage="kmeans")
    feed(startep, endep, stage="kmstat", km_setid="km20100101000020201231000096964fd8ed")
    
    #predict()
    #corr()