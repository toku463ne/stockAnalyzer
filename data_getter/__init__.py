BLOCK_SIZE = 10000
BUFFER_BARS = 10
import env

def getDataGetter(codename, granularity, is_dgtest=False):
    dg = None
    for ds in env.conf["data_sources"]:
        if ds == "yf":
            import data_getter.yfinance_getter
            dg = data_getter.yfinance_getter.YFinanceGetter(codename, granularity)
        if ds == "mydf":
            import data_getter.mydf_getter
            dg = data_getter.mydf_getter.MyDfGetter(dg, is_dgtest=is_dgtest)
        if ds == "mysql":
            import data_getter.my_getter
            dg = data_getter.my_getter.MyGetter(dg, is_dgtest=is_dgtest)
    return dg

def getAllTables():
    from db.mysql import MySqlDB
    db = MySqlDB()
    sql = """select table_name from information_schema.tables 
where table_schema = '%s'
and table_name like 'ohlcv_%';
    """ % (db.dbName)

    tableNames = []
    for (tableName,) in db.execSql(sql):
        tableNames.append(tableName)

    return tableNames

class DataGetter(object):
    def __init__(self, childDG):
        self.childDG = childDG
        self.data_get_interval = 0
        if childDG != None:
            self.codename = childDG.codename
            self.granularity = childDG.granularity
            self.unitsecs = childDG.unitsecs
            self.buffer_secs = BUFFER_BARS * self.unitsecs

    # must implement
    def getPrices(self, startep, endep):
        pass

    def _getPricesFromChild(self, startep, endep, waitDownload=True):
        data = None
        while True:
            endep2 = startep + self.unitsecs*BLOCK_SIZE
            if endep2 > endep:
                endep2 = endep
            if data == None:
                data = self.childDG.getPrices(startep, endep2, waitDownload)
            else:
                data = data.append(self.childDG.getPrices(startep, endep2, waitDownload))
            if endep2 >= endep:
                break
        return data

    