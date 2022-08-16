from data_getter.mydf_getter import MyDfGetter
from data_getter import DataGetter
import lib.naming as naming

class MyGetter(DataGetter):
    def __init__(self,childDG, tableNamePrefix="", is_dgtest=False):
        self.name = "mygetter_%s_%s" % (childDG.codename, childDG.granularity)
        self.tableName = naming.priceTable(childDG.codename, childDG.granularity, tableNamePrefix)
        self.mydf = MyDfGetter(childDG, tableNamePrefix, is_dgtest=is_dgtest)
        self.unitsecs = self.mydf.unitsecs

    def getPrices(self, startep, endep, waitDownload=True):
        df = self.mydf.getPrices(startep, endep, waitDownload)
        ep = df.EP.values
        dt = df.DT.values
        o = df.O.values
        h = df.H.values
        l = df.L.values
        c = df.C.values
        v = df.V.values
        return (ep, dt, o, h, l, c, v)
        
    def drop(self):
        self.mydf.drop()
        
    def truncate(self):
        self.mydf.truncate()
