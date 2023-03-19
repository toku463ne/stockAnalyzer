from datetime import datetime

from analyzer.data_manager import DataManager
import lib.naming as naming
import data_getter
import lib
from consts import *

#zz_min_nbars

class CodeManager(DataManager):
    def __init__(self, config):
        super(CodeManager, self).__init__(config)
        self.initAttrFromArgs(config, "granularity", "D")
        self.initAttrFromArgs(config, "zz_size", 5)
        self.initAttrFromArgs(config, "codenames", [])
        self.initAttrFromArgs(config, "zz_min_nbars", 200)
        self.initAttrFromArgs(config, "recreate", False)

        codeTableName = naming.getZzCodeTableName(self.granularity)

        if self.recreate:
            self.updb.dropTable(codeTableName)
        self.ensureTable(codeTableName, "anal_zzcodes")
        

    def getCodes(self, obsyear):
        if len(self.codenames) == 0:
            self.codenames = self._getCodenamesFromDB(obsyear)
        return self.codenames

    def register(self, obsyear):
        year = obsyear
        if year == 0:
            raise Exception("Need to init with startep end endep for initZzCodeTable")
        codenames = self.codenames
        codeTable = self.table
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

        self.codenames = self._getCodenamesFromDB(year)

    

    def _getCodenamesFromDB(self, obsyear, market=""):
        cnt = self.redb.countTable(self.table, ["obsyear = %d" % (obsyear)])
        if cnt == 0:
            return self.codenames

        sql = """select distinct codename from 
%s where obsyear = %d
and min_nth_volume >= %d
and nbars >= %d""" % (self.table, obsyear ,
        ZZ_MIN_VOLUMES, self.zz_min_nbars)

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
    
    