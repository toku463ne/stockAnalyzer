import env


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


def getZzCodeTableName(granularity):
        return "anal_zzcode_%s" % (granularity)

def getZzDataTableName(granularity, n_points):
    return "anal_zzdata_%s_%d" % (granularity, n_points)

def getZzItemTableName(granularity, zz_size, n_points):
    return "anal_zzitems_%s_%d_%d" % (granularity, zz_size, n_points)

def getZzKmStatsTableName(granularity, zz_size, n_points):
    return "anal_zzkmstats_%s_%d_%d" % (granularity, zz_size, n_points)
