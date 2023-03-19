import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import data_getter
from event.tick import TickEvent
from consts import *
from db.mysql import MySqlDB
import lib

class Ticker(object):
    def __init__(self, config):
        self.initAttrFromArgs(config, "codename")
        self.initAttrFromArgs(config, "granularity")
        self.initAttrFromArgs(config, "startep", 0)
        self.initAttrFromArgs(config, "endep", 0)
        self.initAttrFromArgs(config, "buffNbars", 100)
        self.initAttrFromArgs(config, "use_master", False)
        self.initAttrFromArgs(config, "save_db", False)
        self.initAttrFromArgs(config, "recreate", False)
        

        dg = data_getter.getDataGetter(self.codename, self.granularity)
        self.dg = dg
        self.unitsecs = dg.unitsecs
        self.redb = MySqlDB(is_master=self.use_master)
        self.updb = MySqlDB(is_master=self.use_master)

        if self.redb.tableExists("tick_tableeps") == False:
            self.updb.createTable("tick_tableeps", "tick_tableeps")

        self.err = TICKER_ERR_NONE
        
        #self.initData()

    
    def initData(self):
        self._resetData(self.startep, self.endep, use_master=self.use_master)
    

    def ensureTable(self, tableName, tableTemplateName="", replacements={}):
        self.tableTemplate = tableTemplateName
        self.table = tableName
        if self.recreate:
            self.dropTable()

        self.tableReplacements = replacements
        if self.redb.tableExists(tableName) == False:
            self.updb.createTable(tableName, tableTemplateName, replacements)
        



    def dropTable(self):
        sql = "delete from tick_tableeps where table_name = '%s' and codename = '%s';" % (self.table, self.codename)
        self.updb.execSql(sql)
        self.updb.dropTable(self.table)



    def _resetData(self, startep, endep=0, use_master=True):
        if endep == 0:
            endep = startep + self.unitsecs*self.buffNbars
        else:
            endep += self.unitsecs*self.buffNbars
        ohlcv = self.dg.getPrices(startep, endep)
        #if load_db:
        #   self.loadData(ohlcv, startep, endep, use_master=use_master)
        #else:
        #    self.initData(ohlcv)
        self.calcTickValues(ohlcv)
        self.index = -1
        self.err = TICKER_ERR_NONE
        self.data = None

    # must inherit
    def loadData(startep, endep, use_master=True):
        pass


    def getTableEp(self):
        sql = "select startep, endep from tick_tableeps where table_name='%s' and codename='%s';" % (self.table, self.codename)
        res = self.redb.select1rec(sql)
        if res == None:
            return None
        (startep, endep) = res
        return startep, endep


    def updateTableEp(self, startep, endep):
        res = self.getTableEp()
        if res == None:
            startep = min(self.startep, startep)
            endep = max(self.endep, endep)
            sql = """insert into tick_tableeps(table_name, codename, startep, endep, startdt, enddt)
values('%s', '%s', %d, %d, '%s', '%s')""" % (self.table, self.codename, startep, endep, lib.epoch2dt(startep), lib.epoch2dt(endep))
            self.updb.execSql(sql)
            return
        
        (db_startep, db_endep) = res

        if db_startep > startep:
            sql = "update tick_tableeps set startep=%d, startdt='%s' where table_name='%s' and codename='%s';" % (startep, lib.epoch2dt(startep), self.table, self.codename)
            self.updb.execSql(sql)

        if db_endep < endep:
            sql = "update tick_tableeps set endep=%d, enddt='%s' where table_name='%s' and codename='%s';" % (endep, lib.epoch2dt(endep), self.table, self.codename)
            self.updb.execSql(sql)





    # must inherit
    def calcTickValues(self, ohlcv):
        (ep, dt, o, h, l, c, v) = ohlcv
        self.ep = ep
        self.dt = dt
        self.o = o
        self.h = h
        self.l = l
        self.c = c
        self.v = v

    # must inherit
    def getData(self, i=-1, n=1):
        if i == -1 and self.err == TICKER_ERR_NONE:
            i = self.index
        if n == 1 and i >= 0:
            return (self.ep[i], self.dt[i],self.o[i],self.h[i],self.l[i],self.c[i],self.v[i])
        elif i >= 0:
            j = i-n+1
            if j < 0:
                j = 0
            i = i+1
            return (self.ep[j:i], self.dt[j:i],self.o[j:i],
                self.h[j:i],self.l[j:i],self.c[j:i],
                self.v[j:i])
        else:
            return (0,None,0,0,0,0,0)

    def getTickEvent(self):
        if self.tick():
            (ep, _, o, h, l, c, v) = self.data
            return TickEvent(ep, c, o, h, l, c, v)
        else:
            return None

    def getPrevIndex(self, ep, searchStartI=0):
        eps = self.ep
        for i in range(searchStartI, len(ep)):
            if ep < eps[i]:
                return i-1
            if ep == eps[i]:
                return i

    def getPrevEpoch(self, ep, searchStartI=0):
        i = self.getPrevIndex(ep, searchStartI)
        return self.ep[i]

    def getPrice(self, ep):
        if self.tick(ep) == False:
            if self.err == TICKER_ERR_EOF and self.index == -1:
                self._resetData(ep)
            if self.tick(ep) == False:
                raise Exception("No data for epoch=%d" % ep)
        return self.getData()  



    def getPostIndex(self, ep, searchStartI=0):
        eps = self.ep
        for i in range(searchStartI, len(ep)):
            if ep < eps[i]:
                return i
            if ep == eps[i]:
                return i


    def _setCurrData(self, i):
        if i >= 0:
            self.data = self.getData(i, 1)
        else:
            self.data = self.getData(-1)

    def _deleteOld(self, oldi):
        rmi = min(self.index, oldi)
        self.ep = self.ep[rmi+1:]
        self.dt = self.dt[rmi+1:]
        self.o = self.o[rmi+1:]
        self.h = self.h[rmi+1:]
        self.l = self.l[rmi+1:]
        self.c = self.c[rmi+1:]
        self.v = self.v[rmi+1:]
        self.index -= rmi

    def tick(self, ep=0):
        if self.err == TICKER_ERR_EOF:
            self._setCurrData(-1)
            return False
        n = len(self.ep)
        if n == 0:
            return False
        if ep > 0:
            if ep > self.ep[-1]:
                self.err = TICKER_ERR_EOF
                self._setCurrData(-1)
                self.index = -1
                return False
            i = 0
            if self.index >= 0 and ep > self.ep[self.index]: 
                i = self.index
            while i < n:
                if ep == self.ep[i]:
                    self.index = i
                    self._setCurrData(i)
                    self.err = TICKER_ERR_NONE
                    return True
                elif ep < self.ep[i]:
                    if i > 0:
                        j = i - 1
                        self.index = j
                        self._setCurrData(j)

                        if ep - self.unitsecs > self.ep[j]:
                            self.err = TICKER_NODATA
                    else:
                        self.err = TICKER_NODATA
                        self._setCurrData(-1)
                    return True
                i += 1
        else:
            self.index += 1
            if self.index >= n:
                self.err = TICKER_ERR_EOF
                self._setCurrData(-1)
                return False
            i = self.index
            self._setCurrData(i)
            return True
        self.err = TICKER_NODATA
        self._setCurrData(-1)
        return False

    def initAttrFromArgs(self, args, name, default=None):
        if name in args.keys():
            setattr(self, name, args[name])
        elif default is None:
            raise Exception("%s is necessary!" % name)
        else:
            setattr(self, name, default)