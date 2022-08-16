import yaml
import lib
import datetime
import os

BASE_DIR = os.path.dirname(os.path.realpath(__file__))
SQL_DIR = '%s/%s' % (BASE_DIR, "sql")

conf = yaml.load(open("%s/default.yaml" % BASE_DIR), Loader=yaml.FullLoader)
conf["is_test"] = False

def loadConf(confpath):
    global conf
    conf2 = yaml.load(open(confpath), Loader=yaml.FullLoader)
    lib.mergeJson(conf, conf2)

def log(msg):
    now = datetime.datetime.now()
    t = now.strftime("%Y-%m-%d %H:%M:%S")
    msg = "%s | %s" % (t, str(msg))
    print(msg)

def printDebug(ep, msg):
    if conf.loglevel <= conf.LOGLEVEL_DEBUG:
        printMsg(ep, "DEBUG | %s" % msg)
        
def printInfo(ep, msg):
    if conf.loglevel <= conf.LOGLEVEL_INFO:
        printMsg(ep, "INFO  | %s" % msg)

def printError(ep, msg):
    if conf.loglevel <= conf.LOGLEVEL_ERROR:
        printMsg(ep, "ERROR | %s" % msg)

def printMsg(ep, msg):
    timestr = ""
    if conf.run_mode in [conf.MODE_BACKTESTING, 
                        conf.MODE_UNITTEST,
                        conf.MODE_SIMULATE]:
        timestr = lib.epoch2str(ep, conf.DATE_FORMAT_NORMAL)
    else:
        timestr = lib.epoch2str(lib.nowepoch(), conf.DATE_FORMAT_NORMAL)
    print("%s %s" % (timestr, msg))