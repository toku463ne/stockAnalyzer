# Do NOT import env
import datetime, calendar, time, pytz
import os
DEFAULT_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

def epoch2dt(epoch):
    d = datetime.datetime.utcfromtimestamp(epoch)
    return d

def dt2epoch(gmdt):
    return int(calendar.timegm(gmdt.timetuple()))

def str2dt(strgmdt, format=DEFAULT_DATETIME_FORMAT):
    d = datetime.datetime.strptime(strgmdt, format)
    d = pytz.utc.localize(d)
    return d
    
def str2epoch(strgmdt, format=DEFAULT_DATETIME_FORMAT):
    return dt2epoch(str2dt(strgmdt, format))
    
def dt2str(gmdt, format=DEFAULT_DATETIME_FORMAT):
    return gmdt.strftime(format)

def epoch2str(epoch, format=DEFAULT_DATETIME_FORMAT):
    return dt2str(epoch2dt(epoch), format)

def nowepoch():
    return time.time()

def list2str(list1, sep=",", enquote=False):
    s = ""
    for v in list1:
        if enquote:
            v = "'%s'" % str(v)
        if s == "":
            s = v
        else:
            s = "%s%s %s" % (s, sep, str(v))
    return s


def mergeJson(j1, j2):
    if not isinstance(j1, dict) or not isinstance(j2, dict):
        return j2

    if len(j1) == 0 or len(j2) == 0:
        return j2
    for k1 in j1.keys():
        for k2 in j2.keys():
            if k1 == k2:
                j1[k1] = mergeJson(j1[k1], j2[k2])
                break
    
    return j1

def ensureDir(path):
    if not os.path.exists(path):
        os.makedirs(path)
