from db.mysql import MySqlDB


class Analyzer(object):
    def __init__(self):
        self.prepareTables()

    def prapareTables(self):
        db = MySqlDB()
        for t in self.tableNames:
            if db.tableExists(t) == False:
                db.createTable(t)

    def initAttrFromArgs(self, args, name, default=None):
        if name in args.keys():
            setattr(self, name, args[name])
        elif default is None:
            raise Exception("%s is necessary!" % name)
        else:
            setattr(self, name, default)