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


