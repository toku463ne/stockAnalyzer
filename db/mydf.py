import sqlalchemy

import db
import db.mysql as mydb
import pandas as pd
from env import *
import time
conn_retry = conf["mysql"]["conn_retry"]

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
        return sqlalchemy.create_engine(connectstr, pool_size=20, max_overflow=0)
        # , poolclass=sqlalchemy.pool.NullPool
        # , pool_recycle=10
        

    def getConn(self):
        return self.getEngine().connect()

    def read_sql(self, sql):
        time_to_sleep = 1
        df = None
        cnt = 1
        while cnt <= conn_retry:
            try:
                db = self.getEngine()
                conn = db.connect()
                df = pd.read_sql(sql, conn)
                conn.close()
                db.dispose()
            except Exception as e:
                if cnt >= conn_retry:
                    log("tried to connect %d times" % cnt)
                    raise e
                log(e)
                time.sleep(time_to_sleep)
                time_to_sleep += 1
            cnt += 1
        if df is None:
            raise Exception("couldn't get dataframe")
        return df