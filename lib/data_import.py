import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from env import *
import data_getter
from db.mysql import MySqlDB
import time
import lib.naming as naming
import lib
import json


def get_random_codes(cnt):
    mydb = MySqlDB()
    sql = """SELECT c1.codename FROM codes AS c1 
    JOIN (SELECT codename FROM codes ORDER BY RAND() LIMIT %d) as c2 
    ON c1.codename=c2.codename""" % cnt
    codes = []
    for (code,) in mydb.execSql(sql):
        codes.append(code)

    return codes



def import_codes(codes, startep, endep, granularity, 
    interval=5, is_dgtest=False):
    for instrument in codes:
        log("Getting %s.." % instrument)
        dg = data_getter.getDataGetter(instrument, granularity, is_dgtest=is_dgtest)
        dg.getPrices(startep, endep)
        time.sleep(interval)

    log("completed")


def run(jsonfile="default_import_data.json"):
    import env
    data = ""
    with open("%s/%s" % (env.BASE_DIR, jsonfile), "r") as f:
        data = json.load(f)
    
    codes = data["codenames"]
    granularity = data["granularity"]
    start = data["start_date"]
    end = data["end_date"]
    if len(start) == 10:
        start = start + "T00:00:00"
        end = end + "T00:00:00"

    startep = lib.str2epoch(start)
    endep = lib.str2epoch(end)

    is_test = False
    if "is_test" in data.keys():
        env.conf["is_test"] = data["is_test"]
        is_test = data["is_test"]

    random_cnt = 0
    if "random_cnt" in data.keys():
        random_cnt = int(data["random_cnt"])

    if random_cnt > 0:
        codes.extend(get_random_codes(random_cnt))

    import_codes(codes, startep, endep, granularity, 10, is_dgtest=is_test)
    



def get_count(code, startep, endep, granularity):
    mydb = MySqlDB()
    tableName = naming.priceTable(code, granularity)
    cnt = mydb.countTable(tableName, 
    ["EP >= %d" % startep,
    "EP <= %d" % endep])
    return cnt


if __name__ == "__main__":
    run()
