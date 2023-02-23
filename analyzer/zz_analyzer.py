
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
zz_min_nbars = 200

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
        self.initAttrFromArgs(config, "zz_middle_size", 2)
        self.initAttrFromArgs(config, "km_granularity", "Y")
        self.initAttrFromArgs(config, "km_avg_size", 10)
        self.initAttrFromArgs(config, "km_max_k", 8)
        self.initAttrFromArgs(config, "km_min_size", 100)
        self.initAttrFromArgs(config, "km_max_size", 1000)
        self.initAttrFromArgs(config, "codenames", [])
        self.initAttrFromArgs(config, "deflect_rate", 0.65)
        self.initAttrFromArgs(config, "min_cnt_a_year", 10)
        self.initAttrFromArgs(config, "km_setname", "")
        self.initAttrFromArgs(config, "n_candles", 3)

        
        data_dir = ""
        if "data_dir" in config.keys():
            data_dir = config["data_dir"]
        self.data_dir = lib.ensureDataDir(data_dir, subdir="zz")

        self.redb = MySqlDB(is_master=use_master)
        self.updb = MySqlDB(is_master=use_master)
        self.df = mydf.MyDf(is_master=use_master)
        self.kmpeaks = {}
        self.kmcandles = {}
        
        if self.km_setname != "":
            self.km_setid = self.getKmSetId(self.km_setname)
        else:
            self.km_setid = -1        


        granularity = self.granularity
        zz_size = self.zz_size
        n_points = self.n_points

        setIdsTable = "anal_zzkmsetids"
        self.ensureTable("setIdsTable", setIdsTable)

        dataTableName = naming.getZzDataTableName(granularity, zz_size)
        self.ensureTable("dataTable", dataTableName, "anal_zzdata")

        codeTableName = naming.getZzCodeTableName(granularity)
        self.ensureTable("codeTable", codeTableName, "anal_zzcodes")

        kmGroupsTableName = naming.getZzKmGroupsTableName(granularity, zz_size, n_points)
        self.ensureTable("kmGroupsTable", kmGroupsTableName, "anal_zzkmgroups")

        kmGroupsPredictedTableName = naming.getZzKmGroupsPredictedTableName(granularity, zz_size, n_points)
        self.ensureTable("kmGroupsPredictedTable", kmGroupsPredictedTableName, "anal_zzkmgroups")

        
        xys = ""
        stats_xys = ""
        xy = []
        stat_xy = []
        for i in range(n_points+self.zz_middle_size):
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

        candle_fields = ""
        for i in range(self.n_candles):
            if candle_fields != "":
                candle_fields += ","
            candle_fields += "`o%d` FLOAT, `h%d` FLOAT, `l%d` FLOAT, `c%d` FLOAT" % (i,i,i,i)

        candleTableName = naming.getZzCandleTableName(granularity, self.n_points, self.n_candles)
        self.ensureTable("candleTable", candleTableName, 
            "anal_zzcandles", {"#OHLCVCOLUMS#": candle_fields})
        
        kmCandlePredictedTableName = naming.getZzKmCandlePredictedTableName(granularity, zz_size, n_points)
        self.ensureTable("kmCandlePredictedTable", kmCandlePredictedTableName, "anal_zzkmcandlegroups")

        kmCandleStatsTableName = naming.getZzKmCandleStatsTableName(granularity, zz_size, self.n_candles)
        self.ensureTable("kmCandleStatsTable", kmCandleStatsTableName, "anal_zzkmcandlestats")

        self.codenames = self.getCodenamesFromDB()

    def dropAllTables(self):
        for tableName in [self.setIdsTable, self.dataTable, self.codeTable, 
            self.kmGroupsTable, self.itemTable, self.kmStatsTable, self.candleTable, self.kmCandlePredictedTable, self.kmCandleStatsTable]:
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
            
    def getCodenamesFromDB(self, year=0, market=""):
        if year==0:
            year = self.year
        cnt = self.redb.countTable(self.codeTable, ["obsyear = %d" % (year)])
        if cnt == 0:
            return self.codenames

        sql = """select distinct codename from 
%s where obsyear = %d
and min_nth_volume >= %d
and nbars >= %d""" % (self.codeTable, year ,
        ZZ_MIN_VOLUMES, zz_min_nbars)

        if len(self.codenames) > 0:
            sql += " and codename in ('%s')" % "','".join(self.codenames)

        if market != "":
            sql += " and market='%s'" % market
        sql += " order by codename;"
        codenames = []
        for (codename,) in self.redb.execSql(sql):
            codenames.append(codename)
        self.codenames = codenames
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
        for i in range(int(len(vals)/2)):
            ep.append(vals[2*i])
            prices.append(vals[2*i+1])
        return (ep, prices)


    def _normalizeItem(self, ep, prices):
        st = ep[0]
        if len(prices) > self.n_points+1:
            ed = ep[-2]
            ma = max(prices[:-1])
            mi = min(prices[:-1])
        elif len(prices) == self.n_points+1:
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

    def _normalizeCandleItems(self, ohlc, last_val=0):
        mi = min(ohlc)
        g = max(ohlc) - mi
        
        if last_val > 0:
            ohlc.append(last_val)

        ohlc = np.array(ohlc)
        ohlc -= mi
        if g > 0:
            ohlc /= g
            ohlc = np.nan_to_num(ohlc)
        else:
            return None, 0
        ohlc = ohlc.tolist()
        if last_val > 0:
            last_val = ohlc.pop()
        return ohlc, last_val


    def predictNext(self, ep, prices):
        norm_vals = self._normalizeItem(ep, prices)
        if norm_vals == None:
            return (-1,-1, "", -1, -1)
        km_name = self._getKmName(norm_vals)
        km_groupid = 0
        if km_name in self.kmpeaks.keys():
            km_groupid = self.kmpeaks[km_name].predict([norm_vals])[0]
        km_id = naming.getKmId(km_name, km_groupid)
        
        sql = """select meanx, meany, score1*score2 score, item_cnt  from %s
where km_id = '%s' and km_setid = %d;""" % (self.kmStatsTable, km_id, self.km_setid)

        res = self.redb.select1rec(sql)
        if res == None:
            return (-1,-1, km_id, -1, -1)
        (meanx, meany, score, item_cnt) = res

        gapx = ep[-1] - ep[0]
        gapy = max(prices) - min(prices)

        x = gapx*meanx
        y = gapy*meany

        return (x, y, km_id, item_cnt, score)

    def predictCandleNext(self, ohlcs):
        norm_vals, _ = self._normalizeCandleItems(ohlcs)
        if norm_vals == None:
            return (-1,-1, "", -1, -1)
        km_name = self._getKmCandleName(norm_vals)
        km_groupid = 0
        if km_name in self.kmcandles.keys():
            km_groupid = self.kmcandles[km_name].predict([norm_vals])[0]
        km_candleid = naming.getKmId(km_name, km_groupid)

        
        sql = """select mean, score, item_cnt  from %s
where km_candleid = '%s' and km_setid = %d;""" % (self.kmCandleStatsTable, km_candleid, self.km_setid)

        res = self.redb.select1rec(sql)
        if res == None:
            return (-1, km_candleid, -1, -1)
        (x, score, item_cnt) = res
        return x, km_candleid, item_cnt, score


    def _registerItem(self, codename, st, ed, ep, prices, dirs,
        with_prediction=False):
        #st = ep[0]
        #ed = ep[-1]
        
        sql = """select count(*) from %s
where
codename = '%s'
and startep = %d 
        """ % (self.itemTable, codename, st)
        (cnt,) = self.redb.select1rec(sql)
        zzitemid = -1
        if cnt > 0:
            zzitemid = self.getItemId(codename, st)

        vals = self._normalizeItem(ep, prices)
        if vals == None:
            return
        
        
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
            

        if zzitemid == -1:
            sql = """insert into %s(codename, startep, endep, %s, last_dir) 
        values('%s', 
                %d, %d, 
                %s, %d);
                    """ % (self.itemTable, sqlcols, 
                            codename, 
                            st, ed, 
                            sqlvals, dirs[self.n_points-2])
            
            self.updb.execSql(sql)
            zzitemid = self.getItemId(codename, st)

        if with_prediction:
            (_, _, km_id, _, _) = self.predictNext(ep[:-1], prices[:-1])
            sql = """insert into %s(zzitemid, km_setid, km_id)
values(%d, '%s', '%s');""" % (self.kmGroupsPredictedTable, zzitemid, self.km_setid, km_id)
            self.updb.execSql(sql)


        return zzitemid
    

    def registerData(self, with_prediction=False, is_update=False):
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
            if res != None and is_update==False:
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
            middle_size = zz.middle_size

            if len(zz.ep) < self.n_points:
                continue

            zz.initData(ohlcv, self.zz_size)
            #(eps, dts, o, h, l, c, v) = ohlcv    
            
            zz_ep = zz.zz_ep
            zz_dt = zz.zz_dt
            zz_prices = zz.zz_prices
            zz_v = zz.zz_v
            zz_dirs = zz.zz_dirs
            zz_dists = zz.zz_dists
            zz_indexes = zz.tick_indexes
            o = zz.o
            h = zz.h
            l = zz.l
            c = zz.c

            candle_cols = ""
            for i in range(self.n_candles):
                if candle_cols != "":
                    candle_cols += ","
                candle_cols += "o%d, h%d, l%d, c%d" % (i,i,i,i)

            for i in range(len(zz_ep)):
                sql = """replace into 
%s(codename, EP, DT, P, V, dir, dist) 
values('%s', %d, '%s', %f, %f, %d, %d);""" % (self.dataTable, 
                codename, zz_ep[i], zz_dt[i], zz_prices[i], zz_v[i], zz_dirs[i], zz_dists[i])
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
                        j = i + 1
                        last_feed_ep = zz_ep[j]
                        last_feed_price = zz_prices[j]
                        last_feed_dir = zz_dirs[j]
                        last_feed_zz_i = zz_indexes[j]
                        last_feed_i = j
                        last_middle_i = last_feed_zz_i + middle_size
                        last_i = -1
                        j += 1
                        while j < len_zz_ep-1:
                            if zz_indexes[j] > last_middle_i:
                                last_i = j
                                break
                            j += 1
                        if last_i == -1:
                            break

                        #(last_middle_peak_ep, last_middle_peak_price) = self.getLastMiddlePeakValues(last_feed_dir, last_feed_zz_i, eps, h, l)
                        (last_middle_peak_ep, last_middle_peak_price, last_ep, last_price) = zz.getLastMiddlePeak(last_feed_i)
                        
                        

                        zzitemid = self._registerItem(codename, f_ep[0], last_feed_ep, 
                            f_ep + [last_feed_ep, last_middle_peak_ep, last_ep, zz_ep[last_i]],  
                            f_prices + [last_feed_price, last_middle_peak_price, last_price, zz_prices[last_i]], 
                            f_dirs + [last_feed_dir, 0, 0, zz_dirs[last_i]], 
                            with_prediction=with_prediction)

                        if zzitemid != None:
                            # register candles
                            candle_vals = []
                            for k in range(self.n_candles):
                                candle_vals = [o[last_feed_zz_i+k-1],h[last_feed_zz_i+k-1],l[last_feed_zz_i+k-1],c[last_feed_zz_i+k-1]] + candle_vals
                            

                            candle_vals, last_val = self._normalizeCandleItems(candle_vals, zz_prices[last_i])
                            if candle_vals != None:
                                candle_vals.append(last_val)
                                sql = "replace into %s(zzitemid, codename, EP, dir, %s, next_peak) values(%d, '%s', %d, %d, %s);" % (self.candleTable, 
                                candle_cols, zzitemid, codename, zz_ep[j], zz_dirs[j], ",".join(map(str,candle_vals)))
                                self.updb.execSql(sql)

                        f_ep.pop(0)
                        f_prices.pop(0)
                        f_dirs.pop(0)
                i +=1
        log("Zigzag registration completed!")

    def registerKmSetId(self, km_setname, startep, endep, obsyear):
        sql = """insert into anal_zzkmsetids(km_setname, startep, endep, obsyear)
values('%s', %d, %d, %d)
""" % (km_setname, startep, endep, obsyear)
        self.updb.execSql(sql)
        
        sql = "select km_setid from anal_zzkmsetids where km_setname = '%s';" % (km_setname)
        (km_setid,) = self.redb.select1rec(sql)
        return km_setid

    def getKmSetId(self, km_setname):
        self.km_setname = km_setname
        sql = "select km_setid from anal_zzkmsetids where km_setname = '%s';" %(km_setname)
        res = self.redb.select1rec(sql)
        if res == None:
            return self.registerKmSetId(km_setname, self.startep, self.endep, self.year)
        else:
            (km_setid,) = res
        return km_setid

    def _getPeakKmSize(self, n):
        k = min(int(n/self.km_avg_size), self.km_max_k)
        if k == 0:
            return 1
        if n/k > self.km_max_size:
            k = math.ceil(n/self.km_min_size)
        return k
        

    def _getCandleKmSize(self, n):
        return math.ceil(n/self.km_min_size)

    def _execKmeans(self, km_setname, kmNameFunc, getKfunc, df):
        zzitemids = df["zzitemid"].tolist()
        del df["zzitemid"]

        km_groups = {}
        item_groups = {}
        kms = {}
        vals = df.values
        for i in range(len(vals)):
            v = vals[i]
            km_name = kmNameFunc(v)
            if km_name not in km_groups.keys():
                km_groups[km_name] = []
                item_groups[km_name] = []
            km_groups[km_name].append(v)
            item_groups[km_name].append(zzitemids[i])
            
        for km_name in km_groups.keys():
            vals = km_groups[km_name]
            k = getKfunc(len(vals))
            if k > 1:
                km = KMeans(n_clusters=k, n_init=10)
                km = km.fit(vals)
                #kmg = km.fit_predict(vals)
                kms[km_name] = km
                # save kmeans model
                pklfile = self._getKmpklFilename(km_setname, km_name)
                with open(pklfile, "wb") as f:
                    pickle.dump(km, f)
            else:
                #kmg = [0]*len(vals)
                kms[km_name] = None
        
        return item_groups, kms

    def execPeakKmeans(self, km_setname=""):
        startep = self.startep
        endep = self.endep
        if startep == 0 or endep == 0:
            raise Exception("Need to init with startep end endep for execKmeans")
        codenames = self.codenames
        if km_setname == "":
            if self.km_setid == -1:
                km_setname = naming.getZzKmSetName(startep, endep, codenames)
                km_setid = self.registerKmSetId(km_setname, startep, endep, self.year)
            else:
                km_setname = self.getKmSetName(self.km_setid)
                km_setid = self.km_setid
        else:
            km_setid = self.getKmSetId(km_setname)
        self.km_setid = km_setid
        self.km_setname = km_setname
        
        
        log("Starting kmeans calculation")
        '''
        df: index:  zzitemid
            values: x1,y1, x2,y2, ...
        '''
        sql = "select zzitemid"
        for i in range(self.n_points-1 + self.zz_middle_size):
            sql += ", x%d, y%d" % (i, i)
        
        sql += " from %s where codename in ('%s')" % (self.itemTable, 
            "','".join(codenames))
        
        sql += " and startep >= %d and endep <= %d" % (startep, endep)
        df = pd.read_sql(sql, self.df.getConn())
        item_groups, kms = self._execKmeans(km_setname, self._getKmName, self._getPeakKmSize, df)

        """
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
                km = KMeans(n_clusters=k, n_init=10)
                km = km.fit(vals)
                kmg = km.fit_predict(vals)
                # save kmeans model
                pklfile = self._getKmpklFilename(km_setname, km_name)
                with open(pklfile, "wb") as f:
                    pickle.dump(km, f)
            else:
                kmg = [0]*len(vals)
        """

        for km_name in item_groups.keys():
            item_group = item_groups[km_name]
            km = kms[km_name]
            if km == None:
                kmg = [0]*(len(item_group))
            else:
                kmg = km.labels_
            for i in range(len(item_group)):
                #sql = "update %s set km_groupid = '%s', km_mode = %d where zzitemid = %d;" % (self.itemTable, 
                #        self._getKmGroupId(gid, kmg[i]), ZZ_KMMODE_FEEDED, item_group[i])
                #print(sql)

                km_id = naming.getKmId(km_name, kmg[i])
                sql = """insert into %s(zzitemid, km_id, km_setid)  
values(%d, '%s', %d)
on duplicate key update 
`km_id` = values(`km_id`)
;""" % (self.kmGroupsTable,
                    item_group[i], km_id, self.km_setid)

                self.updb.execSql(sql)
        self.kmpeaks = kms

        log("Completed! km_setname=%s" % km_setname)
        return km_setname

    def execCandleKmeans(self, km_setname):
        km_setid = self.getKmSetId(km_setname)
        startep = self.startep
        endep = self.endep
        if startep == 0 or endep == 0:
            raise Exception("Need to init with startep end endep for execCandleKmeans")
        codenames = self.codenames
        log("Starting kmeans candle calculation")

        sql = "select zzitemid"
        for i in range(self.n_candles):
            sql += ", o%d, h%d, l%d, c%d" % (i, i, i, i)
        
        sql += " from %s where codename in ('%s')" % (self.candleTable, 
            "','".join(codenames))        
        sql += " and EP >= %d and EP <= %d" % (startep, endep)

        df = pd.read_sql(sql, self.df.getConn())
        item_groups, kms = self._execKmeans(km_setname, self._getKmCandleName, self._getCandleKmSize, df)

        """
        km = KMeans(k)
        km = km.fit(df)
        kmg = km.fit_predict(df)
        # save kmeans model
        pklfile = self._getKmpklFilename(km_setname, naming.getKmCandleName())
        with open(pklfile, "wb") as f:
            pickle.dump(km, f)
        """

        for km_name in item_groups.keys():
            item_group = item_groups[km_name]
            km = kms[km_name]
            if km == None:
                kmg = [0]*(len(item_group))
            else:
                kmg = km.labels_
            for i in range(len(item_group)):
                #sql = "update %s set km_groupid = '%s', km_mode = %d where zzitemid = %d;" % (self.itemTable, 
                #        self._getKmGroupId(gid, kmg[i]), ZZ_KMMODE_FEEDED, item_group[i])
                #print(sql)

                km_candleid = naming.getKmId(km_name, kmg[i])
                sql = """insert into %s(zzitemid, km_candleid, km_setid)  
values(%d, '%s', %d)
on duplicate key update 
`km_candleid` = values(`km_candleid`)
;""" % (self.kmGroupsTable,
                    item_group[i], km_candleid, km_setid)

                self.updb.execSql(sql)
        
        self.kmcandles = kms



    def _getKmName(self, v):
        def get_b(d):
            b = "0"
            if abs(d) >= 0.01:
                if d > 0:
                    b = "1"
                else:
                    b = "2"
            return b

        if v is None:
            return ""

        n1 = self.n_points-2
        n2 = self.n_points-3
        n3 = self.n_points-4
        g = [0]*(n1 + n2 + n3)
        for j in range(n1):
            d = v[j*2+3]-v[j*2+1]
            g[j] = get_b(d)
        starti = n1
        for j in range(n2):
            d = v[j*2+5]-v[j*2+1]
            g[starti+j] = get_b(d)
        starti = n1+n2
        for j in range(n3):
            d = v[j*2+7]-v[j*2+1]
            g[starti+j] = get_b(d)
        
        gid = "".join(g)
        return "pek" + gid

    def _getKmCandleName(self, v):
        def get_b(a):
            pa = {}
            v = []
            for i in range(len(a)):
                vi = int(a[i]*10)
                pa[vi] = vi
                v.append(vi)
            
            keys = list(pa.keys())
            keys.sort()
            i = 0
            for p in keys:
                pa[p] = i
                i += 1

            v2 = []
            for b in v:
                v2.append(str(pa[b]))
            
            return "".join(v2)

        h = []
        l = []
        for i in range(int(len(v)/4)):
            h.append(v[i*4+1])
            l.append(v[i*4+2])
        return "cdl" + get_b(h) + get_b(l)


    def getKmSetName(self, km_setid):
        sql = "select km_setname from anal_zzkmsetids where km_setid = %d;" % (km_setid)
        (km_setname,) = self.redb.select1rec(sql)
        return km_setname

    def _getKmpklFilename(self, km_setname, km_name):
        km_dir = "%s/%s" % (self.data_dir, km_setname)
        lib.ensureDir(km_dir)
        return "%s/%s.pkl" % (km_dir, km_name)



    def loadKmModel(self):
        def load(kms, km_id):
            (km_name, km_groupid) = naming.extendKmId(km_id)
            kmf = self._getKmpklFilename(self.km_setname, km_name)
            if os.path.exists(kmf):
                with open(kmf, "rb") as f:
                    kms[km_name] = pickle.load(f)


        # to load
        self.kmpeaks = {}
        sql = "select distinct km_id from %s where km_setid=%d;" % (self.kmGroupsTable, self.km_setid)
        for (km_id,) in self.redb.execSql(sql):
            load(self.kmpeaks, km_id)
            """
            (km_name, km_groupid) = naming.extendKmId(km_id)
            kmf = self._getKmpklFilename(self.km_setname, km_name)
            if os.path.exists(kmf):
                with open(kmf, "rb") as f:
                    self.kms[km_name] = pickle.load(f)
            """

        # load candle km
        """
        kmf = self._getKmpklFilename(self.km_setname, naming.getKmCandleName())
        if os.path.exists(kmf):
            with open(kmf, "rb") as f:
                self.kmcandle = pickle.load(f)
        """
        self.kmcandles = {}
        sql = "select distinct km_candleid from %s where km_setid=%d and km_candleid is not null;" % (self.kmGroupsTable, self.km_setid)
        for (km_candleid,) in self.redb.execSql(sql):
            load(self.kmcandles, km_candleid)


    def calcKmClusterStats(self, km_setname, deflect_rate=0.65, min_cnt_a_year=10):
        self.km_setid = self.getKmSetId(km_setname)

        feed_years = self.getNFeededYears()
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

        sql10year = """select g.km_id, year(FROM_UNIXTIME(i.startep)) year,
sum(if(i.y%d-i.y%d>0,1,0)) up_cnt, count(g.km_id) cnt
from %s i
left join %s g on i.zzitemid = g.zzitemid
where g.km_setid = %d
group by km_id, year
having cnt >= %d
""" % (self.n_points+self.zz_middle_size-1,self.n_points+self.zz_middle_size-2,
        self.itemTable,
        self.kmGroupsTable,
        self.km_setid,
        min_cnt_a_year)

        sqlsum = """select km_id, 
sum(if(up_cnt/cnt>%f,1,0)) as u_year_cnt,
sum(if(up_cnt/cnt<%f,1,0)) as d_year_cnt,
ifnull(count(year),0) as year_cnt
from 
(%s) a
group by km_id""" % (deflect_rate, 1-deflect_rate, sql10year)

        itemsql = "select zzitemid, x%d-x%d dx, y%d-y%d dy from %s" % (self.n_points-1, self.n_points-2,
        self.n_points-1, self.n_points-2,
        self.itemTable)

        selsql = """select g.km_id, g.km_setid, count(g.km_id) item_cnt, 
sum(if(i.dy>0,1,0)) item_ucnt,
sum(if(i.dy<0,1,0)) item_dcnt,
ifnull(year_cnt,0) year_cnt,
ifnull(if(u_year_cnt>d_year_cnt, u_year_cnt, d_year_cnt),0)/%d score1, 
greatest(sum(if(i.dy>0,1,0))/count(g.km_id), sum(if(i.dy<0,1,0))/count(g.km_id)) score2,
trimmean(i.dx, .2) mx, 
trimmean(i.dy, .2) my, 
std(i.dx) sx, std(i.dy) sy
from (%s) i
left join %s g on i.zzitemid = g.zzitemid
left join (%s) a on a.km_id = g.km_id
where
g.km_setid = %d
group by km_id
""" % (feed_years,
        itemsql,
        self.kmGroupsTable,
        sqlsum,
        self.km_setid)

        insql = """insert into 
%s(`km_id`, `km_setid`, `item_cnt`, `item_ucnt`, `item_dcnt`, 
`year_cnt`, `score1`, `score2`, `meanx`, `meany`,
`stdx`, `stdy`) 
%s
on duplicate key update 
`item_cnt` = values(`item_cnt`),
`item_ucnt` = values(`item_ucnt`),
`item_dcnt` = values(`item_dcnt`),
`year_cnt` = values(`year_cnt`),
`score1` = values(`score1`),
`score2` = values(`score2`),
`meanx` = values(`meanx`),
`meany` = values(`meany`),
`stdx` = values(`stdx`),
`stdy` = values(`stdy`);
""" % (self.kmStatsTable, selsql)
        self.updb.execSql(insql)

    def calcKmCandleStats(self, km_setname):
        self.km_setid = self.getKmSetId(km_setname)
        sql = "delete from %s where km_setid=%d;" % (self.kmCandleStatsTable, self.km_setid)
        self.updb.execSql(sql)


        sql = """select g.km_candleid, 
count(g.km_candleid) item_cnt,
sum(if(c.dir = -2, 1, 0)) dpeak_cnt,
sum(if(c.dir = -1, 1, 0)) dtrend_cnt,
sum(if(c.dir = 1, 1, 0)) utrend_cnt,
sum(if(c.dir = 2, 1, 0)) upeak_cnt,
trimmean(c.next_peak, 2) mean,
std(c.next_peak) std
from %s g
left join %s c on c.zzitemid = g.zzitemid
where g.km_setid = %d
and g.km_candleid is not null
group by g.km_candleid
""" % (self.kmGroupsTable,
self.candleTable,
self.km_setid)


        sql = """select km_candleid, %d km_setid, item_cnt, dpeak_cnt, dtrend_cnt, utrend_cnt, upeak_cnt, 
greatest(dpeak_cnt, greatest(dtrend_cnt, greatest(utrend_cnt, upeak_cnt)))/item_cnt score, mean, std
from (%s) a
""" % (self.km_setid, sql)

        sql = """insert into %s(km_candleid, km_setid, item_cnt, dpeak_cnt, dtrend_cnt, utrend_cnt, upeak_cnt, score, mean, std) 
%s
;""" % (self.kmCandleStatsTable, sql)
        self.updb.execSql(sql)

        

    def testKm(self, km_setname):
        log("starting peak km test")
        self.km_setid = self.getKmSetId(km_setname)
        xy = ""
        for i in range(self.n_points + self.zz_middle_size -1):
            if xy == "":
                xy = "x%d, y%d" % (i, i)
            else:
                xy += ", x%d, y%d" % (i, i)

        self.loadKmModel()
        sql = """select %s, i.zzitemid, k.km_id from %s i
inner join %s k on i.zzitemid = k.zzitemid
where k.km_setid = %d;""" % (xy, self.itemTable,
self.kmGroupsTable,
self.km_setid)
        for row in self.redb.execSql(sql):
            km_id1 = row[-1]
            zzitemid = row[-2]
            ep = []
            prices = []
            v = []
            #if zzitemid == 2:
            #    print("here")
            for i in range(self.n_points + self.zz_middle_size -1):
                ep.append(row[i*2])
                prices.append(row[i*2+1])

            #if zzitemid == 25:
            #   print("here")
            (_, _, km_id2, _, _) = self.predictNext(ep, prices)
            if km_id1 != km_id2:
                raise Exception("zzitemid=%d km_id not match %s != %s", (zzitemid, km_id1, km_id2))
        log("completed")

    def testKmCandle(self, km_setname):
        log("starting candle km test")
        self.km_setid = self.getKmSetId(km_setname)
        self.loadKmModel()
        v = ""
        for i in range(self.n_candles):
            if v == "":
                v = "o%d, h%d, l%d, c%d" % (i, i, i, i)
            else:
                v += ", o%d, h%d, l%d, c%d" % (i, i, i, i)

        sql = """select %s, i.zzitemid, k.km_candleid from %s i
inner join %s k on i.zzitemid = k.zzitemid
where k.km_setid = %d;""" % (v, self.candleTable,
self.kmGroupsTable,
self.km_setid)
        for row in self.redb.execSql(sql):
            km_id1 = row[-1]
            zzitemid = row[-2]
            v = []
            for i in range(0, self.n_candles*4):
                v.append(row[i])

            #if zzitemid == 25:
            #   print("here")
            (_, km_id2, _, _) = self.predictCandleNext(v)
            if km_id1 != km_id2:
                raise Exception("zzitemid=%d km_id not match %s != %s", (zzitemid, km_id1, km_id2))
        log("completed")
        
    def getKmGroups(self, min_score):
        sql = """select km_id, `count`, score, predict_dir from %s
where `score` >= %f;""" % (self.kmGroupsTable, min_score)
        df = self.df.read_sql(sql)
        return df


    def getNFeededYears(self):
        km_setid = self.km_setid
        sql = """select count(*) from (
select distinct year(FROM_UNIXTIME(startep)) year 
from %s g
left join %s i on i.zzitemid = g.zzitemid
where g.km_setid = %d) a""" % (self.kmGroupsTable, self.itemTable, km_setid)
        (ycnt,) = self.redb.select1rec(sql)
        return ycnt


    """
    Deflection score is 
    """
    def getDeflectedKmGroups(self, deflect_rate=0.65, min_cnt_a_year=10, km_setid=""):
        if km_setid == "":
            km_setid = self.km_setid
        sql = """SELECT g.km_id, year(FROM_UNIXTIME(i.startep)) year,
abs(i.last_dir) dir, 
count(abs(i.last_dir)) cnt
FROM %s g
left join %s i on i.zzitemid = g.zzitemid
where g.km_setid = '%s'
group by g.km_id, year, dir
having cnt >= %d
""" % (self.kmGroupsTable, self.itemTable, km_setid, min_cnt_a_year)

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
        df["score1"] = df["total1"]/df["total"] # middle peak
        df["score2"] = df["total2"]/df["total"] # peak
        max_total = max(df["total"].values)
        df["score"] = df[["score1", "score2"]].max(axis=1) * df["total"] / max_total
        df = df.sort_values("score", ascending=False)

        return df


    def plotKmGroups(self, km_setname, km_ids, vsize=5, hsize=15):
        km_setid = self.getKmSetId(km_setname)
        ncol = 3
        nrow = math.ceil(len(km_ids)/ncol)
        fig = plt.figure(figsize=[hsize,vsize*nrow])
        xs = []
        ys = []
        for i in range(self.n_points):
            xs.append("x%d" % i)
            ys.append("y%d" % i)

        i = 0
        for km_id in km_ids:
            sql = """select i.codename, i.startep, i.endep, 
ks.meanx, ks.meany, ks.stdx, ks.stdy,
%s, %s, abs(x%d-x%d)* abs(y%d-y%d) gap from 
%s as i
left join %s as g 
on g.zzitemid = i.zzitemid      
left join (select km_id, meanx, meany, stdx, stdy from %s) ks
on g.km_id = ks.km_id
where  
g.km_id = '%s'
and g.km_setid = %d
order by gap
limit 100;    
""" % (",".join(xs), ",".join(ys), 
            self.n_points-1,self.n_points-2,
            self.n_points-1,self.n_points-2,
            self.itemTable,
            self.kmGroupsTable,
            self.kmStatsTable,
            km_id,
            km_setid
            )


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

                #if x[-1] > meanx+5*stdx or x[-1] < meanx-5*stdx:
                #    continue
                #if y[-1] > meany+5*stdy or y[-1] < meany-5*stdy:
                #    continue

                legend = "%s %s-%s" % (code, 
                    lib.epoch2str(startep, "%Y-%m-%d"), 
                    lib.epoch2str(endep, "%Y-%m-%d"))

                ax.plot(x, y, label=legend)
                #ax.legend()
                ax.set_title("id=%s" % (km_id))
            i += 1

    def plotCandle(self, km_setid, km_candleid, vsize=5, hsize=15):
        import mplfinance as mpf
        ncol = 3
        nrow = math.ceil(len(km_ids)/ncol)
        fig = plt.figure(figsize=[hsize,vsize*nrow])


def getValue(key, default=None):
    if key in config:
        return config[key]
    elif default is None:
        raise Exception("%s is necessary" % (key))
    else:
        return default

def feed(startep, endep, config={}, stage="all", km_setname="", with_prediction=False):
    a = ZzAnalyzer(startep, endep, config=config, use_master=True)

    if stage == "init" or stage == "all":
        a.initZzCodeTable()

    if stage == "register" or stage == "all":
        a.registerData(with_prediction=with_prediction)

    if stage == "kmeans" or stage == "all":
        km_setname = a.execPeakKmeans()
        if stage == "kmeans":
            return km_setname

    if stage == "kmcandles" or stage == "all":
        a.execCandleKmeans(km_setname)

    
    if stage == "kmstat" or stage == "all":
        a.calcKmClusterStats(km_setname)

    if stage == "kmcandlestat" or stage == "all":
        a.calcKmCandleStats(km_setname)

    if stage == "testkm" or stage == "all":
        a.testKm(km_setname)

    if stage == "testkmcandle" or stage == "all":
        a.testKmCandle(km_setname)


def deflected_kmgroups(km_setid, deflect_rate=0.65, min_cnt_a_year=10):
    a = ZzAnalyzer()
    return a.getDeflectedKmGroups(km_setid=km_setid)

def predict(startep, endep, km_setname, config={}, obsyear=0):
    config["km_setname"] = km_setname
    a = ZzAnalyzer(startep, endep,config=config, use_master=True)
    a.getCodenamesFromDB(obsyear)
    a.registerData(with_prediction=True, is_update=True)
    log("completed")

      
def plot(km_setname, km_ids):
    a = ZzAnalyzer()
    a.plotKmGroups(km_setname, km_ids, vsize=10, hsize=15)
    

def strStEd2ep(start, end):
    if len(start) == 10:
        start = start + "T00:00:00"
        end = end + "T00:00:00"

    startep = lib.str2epoch(start)
    endep = lib.str2epoch(end)

    return startep, endep

if __name__ == "__main__":
    import sys
    start = "2011-01-01"
    end = "2020-12-31"
    stage = "kmstat"
    #stage = sys.argv[0]
    #start = sys.argv[1]
    #end = sys.argv[2]
    
    (startep, endep) = strStEd2ep(start, end)
    config = {
        "granularity": "D",
        "n_points": 5,
        "zz_size": 5,
        "data_dir": "app/stockAnalyzer",
        "codenames": []
    }

    #feed(startep, endep, stage=stage)

    #feed()
    #feed(stage="init", config=config)
    #feed(startep, endep, stage="register", config=config)
    #km_setname = feed(startep, endep, stage="kmeans", config=config)
    #feed(startep, endep, stage="kmstat", km_setname=km_setname)
    
    km_setname = "km2011010100002020123100001f8cc14d73"
    config["km_setname"] = km_setname
    #km_setname = feed(startep, endep, stage="kmeans", config=config)
    #feed(startep, endep, stage="kmstat", km_setname=km_setname)
    
    
    #feed(startep, endep, stage="kmcandles", km_setname=km_setname)
    #feed(startep, endep, stage="kmcandlestat", km_setname=km_setname)
    
    #feed(startep, endep, stage="testkm", km_setname=km_setname)
    #feed(startep, endep, stage="testkmcandle", km_setname=km_setname)
    

    #km_setname = "km2011010100002020123100001f8cc14d73"
    start = "2021-01-01"
    end = "2022-12-31"
    (startep, endep) = strStEd2ep(start, end)
    #feed(startep, endep, stage="register", config=config, with_prediction=True)
    #km_setname = "km20100101000020201231000096964fd8ed"
    #km_setname = "km20110101000020201231000096964fd8ed"
    #predict(startep, endep, km_setname, config, obsyear=2020)
    
    #plot("km20100101000020201231000096964fd8ed", ["1210_0000000004"])