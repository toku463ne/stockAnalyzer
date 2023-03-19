
from db.mysql import MySqlDB

class DataManager(object):
    def __init__(self, config):
        self.initAttrFromArgs(config, "use_master", False)
        self.redb = MySqlDB(is_master=self.use_master)
        self.updb = MySqlDB(is_master=self.use_master)


    def register(self):
        pass

    
    def truncate(self):
        self.updb("truncate table %s" % self.table)



    def ensureTable(self, tableName, tableTemplateName="", replacements={}):
        self.tableTemplate = tableTemplateName
        self.tableReplacements = replacements
        if self.redb.tableExists(tableName) == False:
            self.updb.createTable(tableName, tableTemplateName, replacements)
        self.table = tableName

    def ensureOtherTable(self, table, tableName, tableTemplateName="", replacements={}):
        if self.redb.tableExists(tableName) == False:
            self.updb.createTable(tableName, tableTemplateName, replacements)
        setattr(self, table, tableName)


    def initAttrFromArgs(self, args, name, default=None):
        if name in args.keys():
            setattr(self, name, args[name])
        elif default is None:
            raise Exception("%s is necessary!" % name)
        else:
            setattr(self, name, default)