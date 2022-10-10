from http.client import REQUEST_URI_TOO_LONG
import pandas as pd
import lib.sqlib as sqlib

from data_getter import DataGetter
import lib.naming as naming
import db.mydf as mydf
import db.mysql as mysql


class MyDfGetter(DataGetter):
    def __init__(self, childDG, tableNamePrefix="", is_dgtest=False):
        is_master = True
        if is_dgtest: # always refer to data in the main database
            is_master = False

        super(MyDfGetter, self).__init__(childDG)
        self.childDG = childDG
        self.name = "mydfgetter_%s_%s" % (self.codename, self.granularity)
        self.tableName = naming.priceTable(self.codename, self.granularity, tableNamePrefix)
        self.df = mydf.MyDf(is_master=is_master)
        self.db = mysql.MySqlDB(is_master=is_master)
        if self.db.tableExists(self.tableName) == False:
            self.db.createTable("ohlcvinfo")
            self.db.createTable(self.tableName, "ohlcv")
            sql = """replace into ohlcvinfo(tablename, codename, granularity)
values('%s', '%s', '%s')""" % (self.tableName, self.codename, self.granularity)
            self.db.execSql(sql)

    def getMinMaxEpoch(self):
        (min, max) = self.db.select1rec("select min(EP), max(EP) from %s;" % (self.tableName))
        if min == None:
            min = -1
        if max == None:
            max = -1
        return (int(min), int(max))

    def getData(self, cols=[], condList=[]):
        strWhere = sqlib.list2wheresql(condList)
        strCols = "*"
        if len(cols) > 0:
            strCols = ",".join(cols)
        sql = "select %s from %s %s order by EP;" % (strCols, self.tableName, strWhere)
        df = pd.read_sql(sql, self.df.getConn())
        return df

    def upsertData(self, df):
        if len(df) == 0:
            return
    
        try:
            len(df.index)
        except:
            return

        #df.to_sql(self.tableName, 
        #    self.conn, if_exists='append', index=False)
        sql = "replace into %s(EP, DT, O, H, L, C, V) values" % (self.tableName)
        vals = ""
        for i in range(len(df.index)):
            s = df.iloc[i]
            rowv = "(%i, '%s', %f, %f, %f, %f, %f)" % (s["EP"], 
            s["DT"], s["O"], s["H"], s["L"], s["C"], s["V"])
            if vals == "":
                vals = rowv
            else:
                vals += ",%s" % (rowv)
        sql += vals + ";"
        self.db.execSql(sql)



    def getPrices(self, startep, endep, waitDownload=True):
        upd_startep = startep
        upd_endep = endep
        (minep, maxep) = self.getMinMaxEpoch()
        if startep > maxep and maxep != -1:
            upd_startep = maxep + 1
        else:
            upd_startep -= self.buffer_secs

        if endep < minep and minep != -1:
            upd_endep = minep - 1
        else:
            upd_endep += self.buffer_secs
        
        df = self.getData(condList=["EP >= %d" % startep ,"EP <= %d" % endep])
        need_reselect = False
        if len(df.index) == 0:
            need_reselect = True
            self.upsertData(self._getPricesFromChild(upd_startep, upd_endep, waitDownload))
        else:
            if startep < df.iloc[0]["EP"]:
                need_reselect = True
                self.upsertData(self._getPricesFromChild(upd_startep, df.iloc[0]["EP"]-1, waitDownload))
            if endep > df.iloc[-1]["EP"]:
                need_reselect = True
                self.upsertData(self._getPricesFromChild(df.iloc[-1]["EP"]+1, upd_endep, waitDownload))
        if need_reselect:
            return self.getData(condList=["EP >= %d" % startep ,"EP <= %d" % endep])
        else:
            return df

    def drop(self):
        self.db.dropTable(self.tableName)
        
    def truncate(self):
        self.db.truncateTable(self.tableName)
