
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

class ZzAnalyzer(Analyzer):
    def __init__(self, codenames, granularity, size, startep, endep):
        self.granularity = granularity
        self.size = size
        self.codenames = codenames
        self.tableNames = ["anal_zzgroups", "anal_zzdata"]
        
        self.db = MySqlDB()
        self.df = mydf.MyDf()
        self.startep = startep
        self.endep = endep

        for t in self.tableNames:
            if self.db.tableExists(t) == False:
                self.db.createTable(t)

        xys = ""
        xy = []
        for i in range(size):
            if xys == "":
                xys = "x%d FLOAT, y%d FLOAT" % (i, i)
            else:
                xys += ",x%d FLOAT, y%d FLOAT" % (i, i)
            xy.append(("x%d" % i, "y%d" % i))
        self.xy = xy

        itemTableName = self.getItemTableName()
        if self.db.tableExists(itemTableName) == False:
            self.db.createTable(itemTableName, "anal_zzitems", {"#XYCOLUMS#": xys})
        self.tableNames.append(itemTableName)
        self.itemTable = itemTableName

        statsTableName = self.getKmStatsTableName()
        if self.db.tableExists(statsTableName) == False:
            self.db.createTable(statsTableName, "anal_zzkmstats", {"#XYCOLUMS#": xys})
        self.kmStatsTable = statsTableName

        #self.groupIds = self.getGroupIds()

        

    def getItemTableName(self):
        return "anal_zzitems_%s_%d" % (self.granularity, self.size)

    def getKmStatsTableName(self):
        return "anal_zzkmstats_%s_%d" % (self.granularity, self.size)

    
    def _getGroupId(self, codename):
        sql = """select zzgroupid from anal_zzgroups
where
codename = '%s'
and granularity = '%s'
and size = %d  
        """ % (codename, self.granularity, self.size)
        (zzgroupid,) = self.db.select1rec(sql)
        return zzgroupid

    def getGroupId(self, codename):
        sql = """select count(*) from anal_zzgroups
where
codename = '%s'
and granularity = '%s'
and size = %d  
        """ % (codename, self.granularity, self.size)
        (cnt,) = self.db.select1rec(sql)
        if cnt > 0:
            return str(self._getGroupId(codename))

        sql = """replace into anal_zzgroups(codename, granularity, size)
values('%s', '%s', %d);
        """ % (codename, self.granularity, self.size)

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

    def getGroupIds(self, valtype="str"):
        groupIds = []
        for code in self.codenames:
            groupId = self.getGroupId(code)
            if valtype == "int":
                groupId = int(groupId)
            if valtype == "str":
                groupId = str(groupId)
            
            groupIds.append(groupId)
        return groupIds

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
        lep = ed - st
        ma = max(prices)
        mi = min(prices)
        gap = ma - mi
        if gap == 0:
            return
        
        is_first = True
        for i in range(len(ep)):
            x = (ep[i]-st)/lep
            y = (prices[i]-mi)/gap 
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


    def registerData(self):
        startep = self.startep
        endep = self.endep
        granularity = self.granularity
        first = True

        """
        TODO
        Don't start calculation for already done
        """

        for codename in self.codenames:
            log("Processing %s" % codename)

            zzgroupid = int(self.getGroupId(codename))
            sql = "select startep, endep from %s where "


            dg = data_getter.getDataGetter(codename, granularity)
            if first:
                ohlcv = dg.getPrices(startep, endep, waitDownload=False)
                first = False
            else:
                ohlcv = dg.getPrices(startep, endep, waitDownload=True)
            zz = Zigzag(codename, granularity, startep, endep, size=self.size)

            if len(zz.ep) < self.size:
                continue

            zz.initData(ohlcv, self.size)    
            
            ep = zz.ep
            dt = zz.dt
            prices = zz.prices

            for i in range(len(ep)):
                sql = "replace into anal_zzdata(zzgroupid, EP, DT, P) values(%s, %d, '%s', %f);" % (zzgroupid, ep[i], dt[i], prices[i])
                self.db.execSql(sql)

            for i in range(self.size-1, len(ep)):
                self._registerItem(zzgroupid, 
                    ep[i-self.size+1:i+1], 
                    prices[i-self.size+1:i+1])

        log("Zigzab registration completed!")

    def execKmeans(self, reqGroupIds=[]):
        log("Starting kmeans calculation")

        groupIds = self.getGroupIds()

        '''
        df: index:  zzitemid
            values: x1,y1, x2,y2, ...
        '''
        sql = "select zzitemid"
        for i in range(self.size-1):
            sql += ", x%d, y%d" % (i, i)
        
        sql += " from %s where zzgroupid in (%s)" % (self.itemTable, 
            ",".join(groupIds))
        
        sql += " and startep >= %d and endep <= %d" % (self.startep, self.endep)

        if len(reqGroupIds) > 0:
            sql += " and zzgroupid in (%s)" % (",".join(reqGroupIds))

        df = pd.read_sql(sql, self.df.getConn())
        zzitemids = df["zzitemid"].tolist()
        del df["zzitemid"]

        log("Starting km_groupid calculation")

        k = 6 ** (self.size-1)
        self._updateKmGroupId(k, zzitemids, df, "km_groupid")

        log("Completed!")

    
    def _updateKmGroupId(self, k, zzitemids, df, column_name):
        k = min(int(len(df)/2), k)
        km = KMeans(n_clusters=k)
        km = km.fit(df)
        kmg = km.fit_predict(df)

        print(kmg)
        
        for i in range(len(zzitemids)):
            sql = "update %s set %s = %d where zzitemid = %d;" % (self.itemTable, 
                    column_name, kmg[i], zzitemids[i])
            print(sql)
            self.db.execSql(sql)

        # save kmeans model
        #pickle.dump(km, open("model.pkl", "wb"))

        # to load
        # model = pickle.load(open("model.pkl", "rb"))

    def calcClusterStats(self):
        mxy = ""
        uxy = ""
        cxy = ""
        for (x, y) in self.xy:
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
%s(`km_groupid`, `count`, `mean`, `std`, `last_epoch`, %s)         
(select km_groupid, 
count(km_groupid) c, avg(y%d) m, std(y%d) s, max(endep) last_epoch, %s  
FROM %s item1
inner join (
select min(zzitemid) zzitemid, startep from %s group by startep, km_groupid
) as item2 on item1.zzitemid = item2.zzitemid 
where km_groupid is not null
group by km_groupid
) on duplicate key update 
`count` = values(`count`),
`mean` = values(`mean`),
`std` = values(`std`),
`last_epoch` = values(`last_epoch`),
%s
;""" % (self.kmStatsTable, cxy, 
        self.size-1, self.size-1, mxy, 
        self.itemTable, 
        self.itemTable, 
        uxy)
        self.db.execSql(sql)
        



    def plotTopClusters(self, min_size=30):
        sql = """select km_groupid, 
count(km_groupid) km2cnt, avg(y%d) m, std(y%d) s  
FROM %s
where km_groupid is not null
group by km_groupid
having km2cnt >= %d
order by km2cnt desc
;""" % (self.itemTable, min_size, self.size-1, self.size-1)

        cls = {}
        for (grpid2, cnt, m, s) in self.db.execSql(sql):
            cls[grpid2] = {}
            cls[grpid2]["count"] = cnt
            cls[grpid2]["m"] = m
            cls[grpid2]["s"] = s
            

        xs = []
        ys = []
        for i in range(self.size):
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
                x = np.asarray(tp[3:3+self.size])
                y = np.asarray(tp[3+self.size:3+2*self.size])
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
    size = data["size"]
    granularity = data["granularity"]

    start = data["start_date"]
    end = data["end_date"]
    if len(start) == 10:
        start = start + "T00:00:00"
        end = end + "T00:00:00"

    startep = lib.str2epoch(start)
    endep = lib.str2epoch(end)
    a = ZzAnalyzer(codenames, granularity, size, startep, endep)
    
    if stage == "all" or stage == "zigzag":
        a.registerData()
    
    if stage == "all" or stage == "kmeans" :
        a.execKmeans()

    if stage == "all" or stage == "kmstats":
        a.calcClusterStats()

if __name__ == "__main__":
    #run(stage="zigzag")
    run(stage="kmstats")
