import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from db.mysql import MySqlDB
from env import *
from consts import *

class Classifier(object):
    def __init__(self, config):
        self.initAttrFromArgs(config, "classifier")
        self.initAttrFromArgs(config, "type_name", "")
        self.initAttrFromArgs(config, "valid_after_epoch", 0)
        self.initAttrFromArgs(config, "desc", "")
        self.initAttrFromArgs(config, "use_master", False)
        self.initAttrFromArgs(config, "recreate", False)

        self.redb = MySqlDB(is_master=self.use_master)
        self.updb = MySqlDB(is_master=self.use_master)
        self.ensureTable("idTable", "clf_ids")
        
        _id = self.redb.select1value(self.idTable, "id", ["name = '%s'" % self.classifier])        
        if _id == None:
            if self.valid_after_epoch == 0:
                raise Exception("valid_after_epoch is mandatory when registering a new classifier id!")
            if self.type_name == "":
                raise Exception("type_name is mandatory when registering a new classifier id!")

            sql = """insert into clf_ids(name, type_name, valid_after_epoch, valid_after_dt, `desc`) 
values('%s', '%s', %d, '%s', '%s');""" % (self.classifier, self.type_name, self.valid_after_epoch, lib.epoch2dt(self.valid_after_epoch), self.desc)
            self.updb.execSql(sql)
            _id = self.redb.select1value(self.idTable, "id", ["name = '%s'" % self.classifier])   

        self.id = _id
    
    def getClfId(self, clf_name):
        return self.redb.select1value("clf_ids", "id", ["name = '%s'" % (clf_name)])

    def ensureTable(self, table, tableName, tableTemplateName="", replacements={}):
        if self.redb.tableExists(tableName) == False:
            self.updb.createTable(tableName, tableTemplateName, replacements)
        elif self.recreate:
            self.updb.dropTable(tableName)
            self.updb.createTable(tableName, tableTemplateName, replacements)
        setattr(self, table, tableName)

    def initAttrFromArgs(self, args, name, default=None):
        if name in args.keys():
            setattr(self, name, args[name])
        elif default is None:
            raise Exception("%s is necessary!" % name)
        else:
            setattr(self, name, default)

    def getClassId(self, v):
        pass

    def feed(self, df):
        pass


