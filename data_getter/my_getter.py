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

    def getPrices(self, startep, endep, waitDownload=True, buff_size=0):
        df = self.mydf.getPrices(startep, endep, waitDownload, buff_size=buff_size)
        ep = df.EP.values.tolist()
        dt = df.DT.values.tolist()
        o = df.O.values.tolist()
        h = df.H.values.tolist()
        l = df.L.values.tolist()
        c = df.C.values.tolist()
        v = df.V.values.tolist()
        return (ep, dt, o, h, l, c, v)
        
    def drop(self):
        self.mydf.drop()
        
    def truncate(self):
        self.mydf.truncate()
