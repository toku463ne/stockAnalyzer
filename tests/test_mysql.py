import __init__
import unittest
import db.mysql as mysql

class TestMySQL(unittest.TestCase):
    def test_mysql(self):
        db = mysql.MySqlDB()

        sql = "drop table if exists test_table1;"
        db.execSql(sql)

        sql = "create table if not exists test_table1 (id int, name varchar(10));"
        db.execSql(sql)

        db.truncateTable("test_table1")

        sql = "insert into test_table1 values(1, 'test1');"
        db.execSql(sql)

        cnt = db.countTable("test_table1")
        self.assertEqual(cnt, 1)

        sql = "drop table if exists test_table1;"
        db.execSql(sql)



if __name__ == "__main__":
    unittest.main()
