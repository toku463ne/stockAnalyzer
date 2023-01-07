import pymysql
import db
from env import *

class MySqlDB(db.DB):
	def __init__(self, is_master=False):
		self.is_master = is_master

	def connect(self):
		inf = conf["mysql"]

		condb = inf["db"]
		if self.is_master == False:
			if conf["is_test"]:
				condb = inf["test_db"]

		conn = pymysql.connect(
				host = inf["host"],
				db = condb,
				user = inf["user"],
				passwd = inf["password"],
				charset = "utf8",
				local_infile = 1
		)
		conn.autocommit(True)
		return conn

	def execSql(self, sql):
		cur = None
		try:
			cur = self.connect().cursor()
			cur.execute(sql)
			return cur
		except:
			if cur: cur.close()
			print(sql)
			raise

	def truncateTable(self, tablename):
		try:
			cur = self.conn.cursor()
			sql = "truncate table %s;" % tablename
			return cur.execute(sql)
		except Exception as e:
			printError(sql)
			raise e

	def countTable(self, tablename, whereList=[]):
		strwhere = ""
		if len(whereList) > 0:
			strwhere = "where %s" % (" and ".join(whereList))
		sql = "select count(*) as cnt from %s %s" % (tablename, strwhere)
		cur = self.execSql(sql)
		row = cur.fetchone()
		cur.close()
		return row[0]

	def close(self):
		if self.conn != None:
			self.conn.close()

	def _createTableFromTemplate(self, sqlFile, tableName, replaces={}):
		try:            
			f = open(sqlFile, "r")
			sql = f.read()
			sql = sql.replace("#TABLENAME#", tableName)
			for k in replaces.keys():
				sql = sql.replace(k, replaces[k])
			f.close()
			cur = self.execSql(sql)
			cur.close()
		except:
			raise

	def createTable(self, tableName, templateName="", replaces={}):
		if templateName != "":
			self._createTableFromTemplate("%s/create_table_%s.sql" % (SQL_DIR, 
                                                        templateName), tableName, replaces)
		else:
			self._createTableFromTemplate("%s/create_table_%s.sql" % (SQL_DIR, 
                                                        tableName), tableName)
	
	def tableExists(self, tableName):
		if conf["is_test"]:
			condb = conf["mysql"]["test_db"]
		else:
			condb = conf["mysql"]["db"]
		sql = """SELECT count(*) 
FROM information_schema.TABLES 
WHERE (TABLE_SCHEMA = '%s') AND (TABLE_NAME = '%s');
""" % (condb, tableName)
		(cnt,) = self.select1rec(sql)
		if cnt > 0:
			return True
		else:
			return False


	def dropTable(self, tableName):
		self.execSql("drop table if exists %s;" % tableName)

	def select1rec(self, sql):
		cur = self.execSql(sql)
		row = cur.fetchone()
		cur.close()
		if row:
			return row


	