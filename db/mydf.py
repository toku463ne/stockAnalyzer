import sqlalchemy

import db
import db.mysql as mydb
from env import *

class MyDf(db.DB):
    def __init__(self, is_master=False):
        self.is_master = is_master
        '''
        tableNamePrefix is for testing purpose
        '''
        
    def getEngine(self):
        inf = conf["mysql"]

        condb = inf["db"]
        if self.is_master == False:
            if conf["is_test"]:
                condb = inf["test_db"]

        connectstr = 'mysql+pymysql://%s:%s@%s/%s' % (inf["user"],
        inf["password"],
        inf["host"],
        condb)
        return sqlalchemy.create_engine(connectstr, 
            pool_recycle=3600)
        

    def getConn(self):
        return self.getEngine().connect()
    