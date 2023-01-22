import env
import lib


def formatCodeName(codename):
    codename = codename.replace("^","").replace(".","")
    codename = codename.replace("=X", "USD")
    return codename

def priceTable(codename, granularity, tableNamePrefix=""):
    codename = formatCodeName(codename)
    if tableNamePrefix != "":
        return "%s_ohlcv_%s_%s" % (tableNamePrefix, codename, granularity)
    
    return "ohlcv_%s_%s" % (codename, granularity)

def analyzerTable(anal_type, name):
    return "anal_%s_%s" % (anal_type, name)

def getKmId(km_name, km_groupid):
    return "%s_%s" % (km_name, str(km_groupid).zfill(10))

def extendKmId(km_id):
    s = km_id.split("_")
    # km_name, km_groupid
    return (s[0], s[1])

def getZzCodeTableName(granularity):
        return "anal_zzcode_%s" % (granularity)

def getZzDataTableName(granularity, n_points):
    return "anal_zzdata_%s_%d" % (granularity, n_points)

def getZzItemTableName(granularity, zz_size, n_points):
    return "anal_zzitems_%s_%d_%d" % (granularity, zz_size, n_points)

def getZzKmStatsTableName(granularity, zz_size, n_points):
    return "anal_zzkmstats_%s_%d_%d" % (granularity, zz_size, n_points)

def getZzKmGroupsTableName(granularity, zz_size, n_points):
        return "anal_zzkmgroups_%s_%d_%d" % (granularity, zz_size, n_points)

def getZzKmGroupsPredictedTableName(granularity, zz_size, n_points):
        return "anal_zzkmgroups_predicted_%s_%d_%d" % (granularity, zz_size, n_points)

def getZzKmSetId(startep, endep, codenames):
    import hashlib
    start = lib.epoch2str(startep, "%Y%m%d%H%M")
    end = lib.epoch2str(endep, "%Y%m%d%H%M")
    codestr = "".join(codenames)
    h = hashlib.sha256(codestr.encode()).hexdigest()[:10]
    return "km%s%s%s" % (start, end, h)

