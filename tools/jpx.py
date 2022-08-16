import env
import lib

import requests
from dateutil.parser import parse as parsedate
import os
import datetime
import pandas as pd


def getJPXDf():
    url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
    tmpdir = "%s/stockanaldata" % (env.conf["tmp_dir"])
    dst_file = '%s/data_j.xls' % tmpdir
    lib.ensureDir(tmpdir)
    
    file_time = lib.dt2epoch(datetime.datetime(1900,1,1))
    if os.path.exists(dst_file):
        file_time = lib.dt2epoch(datetime.datetime.fromtimestamp(os.path.getmtime(dst_file)))

    try:
        r = requests.head(url)
        url_datetime = lib.dt2epoch(parsedate(r.headers['Last-Modified']))
        if(url_datetime > file_time):
            env.log("Dowloading data from jpx.co.jp")
            r = requests.get(url)
        
            with open(dst_file, 'wb') as output:
                output.write(r.content)
                os.utime(dst_file, (url_datetime, url_datetime))

    except:
        env.log("Failed to retrieve the last JPX data")

    if os.path.exists(dst_file):
        return pd.read_excel(dst_file) 
    else:
        return None