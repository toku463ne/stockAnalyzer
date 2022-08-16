
from db.mysql import MySqlDB

class MyPartitionedTable(object):
    def __init__(self, tableName, key_column):
        db = MySqlDB()
        db.createTable(tableName)
        self.db = db
        self.tableName = tableName
        self.key_column = key_column

    def getPartitionName(self, key_val):
        return "%s_%s" % (self.tableName, str(key_val))

    def partitionExists(self, partitionName):
        cnt = self.db.countTable("information_schema.partitions", 
            ["table_schema = '%s'" % self.db.dbName,
            "partition_name = '%s'" % partitionName])
        
        if cnt > 0:
            return True

        return False

    def isPartitioned(self):
        sql = """select partition_name from information_schema.partitions
where table_schema = '%s' and table_name = '%s' limit 1;
""" % (self.db.dbName, self.tableName)
        (partitionName,) = self.db.select1rec(sql)
        if partitionName is None:
            return False
        return True


    def addPartition(self, key_val):
        partitionName = self.getPartitionName(key_val)
        if self.partitionExists(partitionName):
            return

        if self.isPartitioned() == False:
            sql = "alter table %s partition by list(%s)" % (self.tableName, self.key_column)
        else:
            sql = "alter table %s add partition" % (self.tableName)

        sql += "(partition %s values in (%s))" % (partitionName, key_val)
        self.db.execSql(sql)


    def dropPartition(self, key_val):
        partitionName = self.getPartitionName(key_val)
        if self.partitionExists(key_val) == False:
            return
        
        sql = "alter table %s drop partition %s;" % (self.tableName, partitionName)
        self.db.execSql(sql)
