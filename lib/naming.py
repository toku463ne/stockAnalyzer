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

def getZigzagTableName(granularity, zz_size, zz_middle_size):
        return "tick_zigzag_%s_%d_%d" % (granularity, zz_size, zz_middle_size)

def getZzCodeTableName(granularity):
    return "anal_zzcodes_%s" % (granularity)

def getZzPeaksTableName(granularity, zz_size, n_points):
    return "anal_zzpeaks_%s_%d_%d" % (granularity, zz_size, n_points)

#def getZzNormPeaksTableName(granularity, zz_size, n_points):
#    return "anal_zznormpeaks_%s_%d_%d" % (granularity, zz_size, n_points)

def getZzKmStatsTableName(granularity, zz_size, n_points):
    return "anal_zzkmstats_%s_%d_%d" % (granularity, zz_size, n_points)

def getZzKmGroupsTableName(granularity, zz_size, n_points):
    return "anal_zzkmgroups_%s_%d_%d" % (granularity, zz_size, n_points)

def getZzKmGroupsPredictedTableName(granularity, zz_size, n_points):
    return "anal_zzkmgroups_predicted_%s_%d_%d" % (granularity, zz_size, n_points)

def getZzCandleTableName(granularity, n_points, n_candles):
    return "anal_zzcandles_%s_%d_%d" % (granularity, n_points, n_candles)

def getZzKmCandleStatsTableName(granularity, zz_size, n_candles):
    return "anal_zzkmcandlestats_%s_%d_%d" % (granularity, zz_size, n_candles)

def getZzKmCandlePredictedTableName(granularity, n_points, n_candles):
    return "anal_zzcandles_predicted_%s_%d_%d" % (granularity, n_points, n_candles)

def getZzKmSetName(startep, endep, codenames, subname=""):
    import hashlib
    start = lib.epoch2str(startep, "%Y%m%d%H%M")
    end = lib.epoch2str(endep, "%Y%m%d%H%M")
    codestr = "".join(codenames)
    h = hashlib.sha256(codestr.encode()).hexdigest()[:10]
    return "km%s%s%s%s" % (subname, start, end, h)

def getKmCandleName():
    return "candles"