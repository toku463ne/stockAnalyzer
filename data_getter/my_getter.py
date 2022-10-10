from data_getter.mydf_getter import MyDfGetter
from data_getter import DataGetter
import lib.naming as naming

class MyGetter(DataGetter):
    def __init__(self,childDG, tableNamePrefix="", is_dgtest=False):
        self.name = "mygetter_%s_%s" % (childDG.codename, childDG.granularity)
        self.codename = childDG.codename
        self.granularity = childDG.granularity
        self.tableName = naming.priceTable(childDG.codename, childDG.granularity, tableNamePrefix)
        self.mydf = MyDfGetter(childDG, tableNamePrefix, is_dgtest=is_dgtest)
        self.unitsecs = self.mydf.unitsecs

    def getPrices(self, startep, endep, waitDownload=True):
        df = self.mydf.getPrices(startep, endep, waitDownload)
        ep = list(df.EP.values)
        dt = list(df.DT.values)
        o = list(df.O.values)
        h = list(df.H.values)
        l = list(df.L.values)
        c = list(df.C.values)
        v = list(df.V.values)
        return (ep, dt, o, h, l, c, v)
        
    def drop(self):
        self.mydf.drop()
        
    def truncate(self):
        self.mydf.truncate()
