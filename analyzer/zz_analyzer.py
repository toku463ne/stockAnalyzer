
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from env import *

from re import X
import data_getter
from analyzer import Analyzer
from ticker.zigzag import Zigzag
from db.mysql import MySqlDB
import db.mydf as mydf
import lib

from sklearn.cluster import KMeans
import pandas as pd
import numpy as np
import json
import matplotlib.pyplot as plt
import math
import pickle

class ZzAnalyzer(Analyzer):
    def __init__(self, granularity, n_points, kmpkl_file, zz_size=5, use_master=False):
        self.granularity = granularity
        self.n_points = n_points
        self.zz_size = zz_size
        self.tableNames = ["anal_zzgroups", "anal_zzdata"]
        self.kmpkl_file = kmpkl_file
        
        self.db = MySqlDB(is_master=use_master)
        self.df = mydf.MyDf(is_master=use_master)
        
        for t in self.tableNames:
            if self.db.tableExists(t) == False:
                self.db.createTable(t)

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

        itemTableName = self.getItemTableName()
        if self.db.tableExists(itemTableName) == False:
            self.db.createTable(itemTableName, "anal_zzitems", {"#XYCOLUMS#": xys})
        self.tableNames.append(itemTableName)
        self.itemTable = itemTableName


        statsTableName = self.getKmStatsTableName()
        if self.db.tableExists(statsTableName) == False:
            self.db.createTable(statsTableName, "anal_zzkmstats", {"#XYCOLUMS#": stats_xys})
        self.kmStatsTable = statsTableName

        #self.groupIds = self.getGroupIds()

        

    def getItemTableName(self):
        return "anal_zzitems_%s_%d" % (self.granularity, self.n_points)

    def getKmStatsTableName(self):
        return "anal_zzkmstats_%s_%d" % (self.granularity, self.n_points)

    
    def _getGroupId(self, codename):
        sql = """select zzgroupid from anal_zzgroups
where
codename = '%s'
and granularity = '%s'
and size = %d  
        """ % (codename, self.granularity, self.zz_size)
        (zzgroupid,) = self.db.select1rec(sql)
        return zzgroupid

    def getGroupId(self, codename):
        sql = """select count(*) from anal_zzgroups
where
codename = '%s'
and granularity = '%s'
and size = %d  
        """ % (codename, self.granularity, self.zz_size)
        (cnt,) = self.db.select1rec(sql)
        if cnt > 0:
            return str(self._getGroupId(codename))

        sql = """replace into anal_zzgroups(codename, granularity, size)
values('%s', '%s', %d);
        """ % (codename, self.granularity, self.zz_size)

        self.db.execSql(sql)
        return self._getGroupId(codename)


    def getItemId(self, zzgroupid, startep):
        sql = """select zzitemid from %s
where
zzgroupid = %d
and startep = %d 
        """ % (self.itemTable,zzgroupid, startep)
        (itemid,) = self.db.select1rec(sql)
        return itemid

    def getGroupIds(self, codenames, valtype="str"):
        groupIds = []
        for code in codenames:
            groupId = self.getGroupId(code)
            if valtype == "int":
                groupId = int(groupId)
            if valtype == "str":
                groupId = str(groupId)
            
            groupIds.append(groupId)
        return groupIds

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
        if len(ep) != self.n_points-1 or len(prices) != self.n_points-1:
            return (-1, -1, -1, -1, -1)

        vals = self._normalizeItem(ep, prices)
        groupid = self.km.predict([vals])[0]
        is_first = True
        sqlcols = ""
        for i in range(len(ep)):
            if is_first == False:
                sqlcols += ", "
            else:
                is_first = False
            sqlcols += "x%d, y%d" % (i, i)
        sql = """select count, meanx, meany, stdx, stdy, %s from %s
where km_groupid = %d;""" % (sqlcols, self.kmStatsTable, groupid)

        res = self.db.select1rec(sql)
        (cnt, meanx, meany, nstdx, nstdy) = res[:5]
        points = res[5:]

        xs = []
        ys = []
        for i in range(int(len(points)/2)):
            xs.append(points[2*i])
            ys.append(points[2*i+1])

        gapx = ep[-1] - ep[0]
        gapy = max(prices) - min(prices)

        (cnt, x, y, stdx, stdy) = (cnt, 
            ep[0] + gapx*meanx, 
            min(prices) + gapy*meany, 
            gapx*nstdx, 
            gapy*nstdy)
        return (cnt, x, y, stdx, stdy, nstdx, nstdy)


    def _registerItem(self, zzgroupid, ep, prices):
        st = ep[0]
        ed = ep[-1]
        
        sql = """select count(*) from %s
where
zzgroupid = %d
and startep = %d 
        """ % (self.itemTable, zzgroupid, st)
        (cnt,) = self.db.select1rec(sql)
        if cnt > 0:
            return self.getItemId(zzgroupid, st)

        sqlcols = ""
        sqlvals = ""
        
        vals = self._normalizeItem(ep, prices)
        if vals == None:
            return

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
            

        sql = """insert into %s(zzgroupid, startep, endep, %s) 
    values(%d, %d, %d, %s);
                """ % (self.itemTable, sqlcols, zzgroupid, st, ed, sqlvals)

        self.db.execSql(sql)
        zzitemid = self.getItemId(zzgroupid, st)

        return zzitemid


    def registerData(self, startep, endep, codenames):
        granularity = self.granularity
        first = True

        """
        TODO
        Don't start calculation for already done
        """

        for codename in codenames:
            log("Processing %s" % codename)

            zzgroupid = int(self.getGroupId(codename))
            sql = "select startep, endep from %s where "


            dg = data_getter.getDataGetter(codename, granularity)
            if first:
                ohlcv = dg.getPrices(startep, endep, waitDownload=False)
                first = False
            else:
                ohlcv = dg.getPrices(startep, endep, waitDownload=True)
            zz = Zigzag(codename, granularity, startep, endep)

            if len(zz.ep) < self.n_points:
                continue

            zz.initData(ohlcv, self.zz_size)    
            
            ep = zz.zz_ep
            dt = zz.zz_dt
            prices = zz.zz_prices

            for i in range(len(ep)):
                sql = "replace into anal_zzdata(zzgroupid, EP, DT, P) values(%s, %d, '%s', %f);" % (zzgroupid, ep[i], dt[i], prices[i])
                self.db.execSql(sql)

            n_points = self.n_points
            for i in range(n_points-1, len(ep)):
                self._registerItem(zzgroupid, 
                    ep[i-n_points+1:i+1], 
                    prices[i-n_points+1:i+1])

        log("Zigzab registration completed!")

    def execKmeans(self, startep, endep, codenames, reqGroupIds=[]):
        log("Starting kmeans calculation")

        groupIds = self.getGroupIds(codenames)

        '''
        df: index:  zzitemid
            values: x1,y1, x2,y2, ...
        '''
        sql = "select zzitemid"
        for i in range(self.n_points-1):
            sql += ", x%d, y%d" % (i, i)
        
        sql += " from %s where zzgroupid in (%s)" % (self.itemTable, 
            ",".join(groupIds))
        
        sql += " and startep >= %d and endep <= %d" % (startep, endep)

        if len(reqGroupIds) > 0:
            sql += " and zzgroupid in (%s)" % (",".join(reqGroupIds))

        df = pd.read_sql(sql, self.df.getConn())
        zzitemids = df["zzitemid"].tolist()
        del df["zzitemid"]

        log("Starting km_groupid calculation")

        k = 6 ** (self.n_points-1)
        self._updateKmGroupId(k, zzitemids, df, "km_groupid")

        log("Completed!")

    
    def _updateKmGroupId(self, k, zzitemids, df, column_name):
        k = min(int(len(df)/2), k)
        km = KMeans(n_clusters=k)
        vals = df.values
        km = km.fit(vals)
        kmg = km.fit_predict(vals)

        # print(kmg)
        
        for i in range(len(zzitemids)):
            sql = "update %s set %s = %d where zzitemid = %d;" % (self.itemTable, 
                    column_name, kmg[i], zzitemids[i])
            #print(sql)
            self.db.execSql(sql)

        # save kmeans model
        with open(self.kmpkl_file, "wb") as f:
            pickle.dump(km, f)
        self.km = km

    def loadKmModel(self):
        # to load
        self.km = pickle.load(open(self.kmpkl_file, "rb"))


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
%s(`km_groupid`, `count`, `meanx`, `meany`, `stdx`, `stdy`, `last_epoch`, %s)         
(select km_groupid, 
count(km_groupid) c, 
avg(x%d) mx, avg(y%d) my, std(x%d) sx, std(y%d) sy, 
max(endep) last_epoch, %s  
FROM %s item1
inner join (
select min(zzitemid) zzitemid, startep from %s group by startep, km_groupid
) as item2 on item1.zzitemid = item2.zzitemid 
where km_groupid is not null
group by km_groupid
) on duplicate key update 
`count` = values(`count`),
`meanx` = values(`meanx`),
`meany` = values(`meany`),
`stdx` = values(`stdx`),
`stdy` = values(`stdy`),
`last_epoch` = values(`last_epoch`),
%s
;""" % (self.kmStatsTable, cxy, 
        self.n_points-1, self.n_points-1, self.n_points-1, self.n_points-1, 
        mxy, 
        self.itemTable, 
        self.itemTable, 
        uxy)
        self.db.execSql(sql)
        



    def plotTopClusters(self, min_size=20):
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
        for (grpid2, cnt, m, s) in self.db.execSql(sql):
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
            sql = """select grp.codename, startep, endep, %s, %s from 
%s as item 
left join anal_zzgroups grp on item.zzgroupid = grp.zzgroupid
where km_groupid = %d;""" % (",".join(xs), 
    ",".join(ys), 
    self.itemTable, grpid2)

            ax = fig.add_subplot(nrow, ncol, i+1)

            for tp in self.db.execSql(sql):
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
                ax.set_title("groupid=%d count=%d \nmean=%f std=%f" % (grpid2, 
                    cls[grpid2]["count"], cls[grpid2]["m"], cls[grpid2]["s"]))
            i += 1



def run(jsonfile="anal_zz_conf.json", stage="all"):
    import env
    data = ""
    with open("%s/%s" % (env.BASE_DIR, jsonfile), "r") as f:
        data = json.load(f)

    codenames = data["codenames"]
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
    
    if stage == "all" or stage == "zigzag":
        a.registerData(startep, endep, codenames)
    
    if stage == "all" or stage == "kmeans" :
        a.execKmeans(startep, endep, codenames=codenames)

    if stage == "all" or stage == "kmstats":
        a.calcClusterStats()

    if stage == "plot":
        a.plotTopClusters()

if __name__ == "__main__":
    run()
    #run(stage="init")
    #run(stage="zigzag")
    #run(stage="kmeans")
    #run(stage="kmstats")
