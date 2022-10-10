
def getUnitSecs(granularity):
    t = granularity[:1]
    i = granularity[1:]
    
    unit_secs = 0
    if t.upper() == "S":
        unit_secs = int(i)
    elif t.upper() == "M" and i != "":
        unit_secs = int(i)*60
    elif t.upper() == "H": 
        unit_secs = int(i)*60*60
    elif t.upper() == "D":
        unit_secs = 60*60*24
    elif t.upper() == "W": 
        unit_secs = 60*60*24*7
    elif t.upper() == "M": 
        unit_secs = 60*60*24
    if unit_secs == 0: raise Exception("Not proper granularity type.")
    return unit_secs


def getMax(ep, p):
    mav = 0
    mat = 0
    for i in range(len(ep)):
        if p[i] > mav:
            mav = p[i]
            mat = ep[i]
    return (mat, mav)
    
def getMin(ep, p):
    miv = 0
    mit = 0
    for i in range(len(ep)):
        if p[i] < miv or miv == 0:
            miv = p[i]
            mit = ep[i]
    return (mit, miv)


def getDecimalPlace(instrument):
    suff = instrument[:-2]
    if suff == ".T":
        return 2