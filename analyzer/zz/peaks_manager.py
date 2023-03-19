
from analyzer.data_manager import DataManager
from classifier.kmpeaks import KmPeaksClassifier
import lib.naming as naming
import lib.tradelib as tradelib
import data_getter
from ticker.zigzag import Zigzag
from db.mydf import MyDf

from env import *


class PeaksManager(DataManager):
    def __init__(self, config):
        super(PeaksManager, self).__init__(config)
        self.initAttrFromArgs(config, "granularity", "D")
        self.initAttrFromArgs(config, "n_points", 5)
        self.initAttrFromArgs(config, "zz_size", 5)
        self.initAttrFromArgs(config, "zz_middle_size", 2)
        self.initAttrFromArgs(config, "codenames", [])
        self.initAttrFromArgs(config, "use_master", False)
        self.initAttrFromArgs(config, "recreate", False)

        xys = ""
        for i in range(self.n_points+self.zz_middle_size):
            if xys == "":
                xys = "x%d FLOAT, y%d FLOAT" % (i, i)
            else:
                xys += ",x%d FLOAT, y%d FLOAT" % (i, i)
            
        peaksTableName = naming.getZzPeaksTableName(self.granularity, self.zz_size, self.n_points)
        self.ensureTable(peaksTableName, "anal_zzpeaks", {"#XYCOLUMS#": xys})
        self.ensureOtherTable("kmPredictedTable", "clf_kmitems_predicted", "clf_kmitems")
        if self.recreate:
            self.updb.dropTable(peaksTableName)
            self.updb.dropTable("clf_kmitems_predicted")
            self.dropAllZzTables()
            self.ensureTable(peaksTableName, "anal_zzpeaks", {"#XYCOLUMS#": xys})
            self.ensureOtherTable("kmPredictedTable", "clf_kmitems_predicted", "clf_kmitems")
        self.kmpeaks = KmPeaksClassifier(config)



    def getDf(self, startep, endep):
        xys = ""
        for i in range(self.n_points+self.zz_middle_size-1):
            if xys == "":
                xys = "x%d, y%d" % (i, i)
            else:
                xys += ",x%d, y%d" % (i, i)

        sql = """select zzitemid, 
from_unixtime(startep) as dt, 
(y%d-y%d) as result, 
%s from %s 
where codename in ('%s') 
and startep>=%d and endep<=%d;""" % (self.n_points+1, self.n_points, 
        xys, 
        self.table, "','".join(self.codenames), startep, endep)
        df_all = MyDf().read_sql(sql)
        df = df_all[["zzitemid", "dt", "result"]]
        df = df.set_index("zzitemid")
        feed_vals = df_all.drop(["zzitemid", "dt", "result"], axis=1).values.tolist()
        return df, feed_vals


    def dropAllZzTables(self):
        zz_config = {
            "granularity": self.granularity,
            "use_master": False,
        }
        for codename in self.codenames:
            zz_config["codename"] = codename
            zz = Zigzag(zz_config)
            zz.dropTable()
        sql = "delete from tick_tableeps where table_name = '%s';" % (self.table)
        self.updb.execSql(sql)

    def register(self, startep, endep, with_prediction=False):
        codenames = self.codenames
        granularity = self.granularity
        unitsecs = tradelib.getUnitSecs(granularity)
        middle_size = self.zz_middle_size

        
        for codename in codenames:
            zz_config = {
                "granularity": granularity,
                "startep": startep,
                "endep": endep,
                "save_db": True,
                "use_master": self.use_master,
            }
            zz_config["codename"] = codename
            zz = Zigzag(zz_config)
            zz.initData()
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
                        
                        reg_ep = f_ep + [last_feed_ep, last_middle_peak_ep, last_ep, zz_ep[last_i]]
                        reg_price = f_prices + [last_feed_price, last_middle_peak_price, last_price, zz_prices[last_i]]
                        zzitemid = self._registerItem(codename, f_ep[0], last_feed_ep, reg_ep, reg_price, 
                            f_dirs + [last_feed_dir, 0, 0, zz_dirs[last_i]])

                        if with_prediction:
                            km_id, _, _, _ = self.predictNext(reg_ep, reg_price)
                            sql = """insert into 
clf_kmitems_predicted(item_id, clf_id, km_id) 
values('%s', %d, '%s');""" % (zzitemid, self.kmpeaks.id, km_id)
                            self.updb.execSql(sql)

                        f_ep.pop(0)
                        f_prices.pop(0)
                        f_dirs.pop(0)
                i +=1

    def getItemId(self, codename, startep):
        sql = """select zzitemid from %s
where
codename = '%s'
and startep = %d 
        """ % (self.table, codename, startep)
        (itemid,) = self.redb.select1rec(sql)
        return itemid


    def normalizeItem(self, ep, prices):
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

    def devideNormalizedVals(self, vals):
        ep = []
        prices = []
        for i in range(int(len(vals)/2)):
            ep.append(vals[2*i])
            prices.append(vals[2*i+1])
        return (ep, prices)


    def _registerItem(self, codename, st, ed, ep, prices, dirs):
        #st = ep[0]
        #ed = ep[-1]
        
        sql = """select count(*) from %s
where
codename = '%s'
and startep = %d 
        """ % (self.table, codename, st)
        (cnt,) = self.redb.select1rec(sql)
        zzitemid = -1
        if cnt > 0:
            zzitemid = self.getItemId(codename, st)

        vals = self.normalizeItem(ep, prices)
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
                    """ % (self.table, sqlcols, 
                            codename, 
                            st, ed, 
                            sqlvals, dirs[self.n_points-2])
            
            self.updb.execSql(sql)
            zzitemid = self.getItemId(codename, st)

        return zzitemid
    

    def execKmeans(self, startep, endep):
        df, feed_vals = self.getDf(startep, endep)
        self.kmpeaks.execKmeans(df, feed_vals)


    def predict(self, v):
        km_id = self.kmpeaks.predict(v)
        sql = "select score, expected, cnt from clf_kminfo where km_id = '%s';" % (km_id)
        res = self.redb.select1rec(sql)
        if res == None:
            return (km_id, -1, 0, 0)
        (score, expected, cnt) = res
        return km_id, score, expected, cnt

    def predictNext(self, ep, prices):
        v = []
        for i in range(self.n_points+self.zz_middle_size-1):
            v.append(ep[i])
            v.append(prices[i])
        return self.predict(v)


    def testKm(self):
        log("starting peak km test")
        xy = ""
        for i in range(self.n_points + self.zz_middle_size -1):
            if xy == "":
                xy = "x%d, y%d" % (i, i)
            else:
                xy += ", x%d, y%d" % (i, i)
        
        self.kmpeaks.loadKmModel()
        sql = """select %s, i.zzitemid, k.km_id from %s i
inner join clf_kmitems k on i.zzitemid = k.item_id
where k.clf_id = %d;""" % (xy, self.table,
self.kmpeaks.id)
        for row in self.redb.execSql(sql):
            km_id1 = row[-1]
            zzitemid = row[-2]
            v = row[:-2]
            km_id2 = self.kmpeaks.predict(v)
            if km_id1 != km_id2:
                raise Exception("zzitemid=%d km_id not match %s != %s", (zzitemid, km_id1, km_id2))
        log("completed")