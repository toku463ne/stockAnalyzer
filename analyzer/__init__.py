from db.mysql import MySqlDB


class Analyzer(object):
    def __init__(self):
        self.prepareTables()

    def prapareTables(self):
        db = MySqlDB()
        for t in self.tableNames:
            if db.tableExists(t) == False:
                db.createTable(t)