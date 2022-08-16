
def list2wheresql(condlist, conjuction="and"):
    strwhere = ""
    for cond in condlist:
        if strwhere == "":
            strwhere = " where %s" % (cond)
        else:
            strwhere = "%s %s %s " % (strwhere, conjuction, cond)
    return strwhere
