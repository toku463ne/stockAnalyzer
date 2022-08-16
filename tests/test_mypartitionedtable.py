import __init__
import unittest
from db.mysql import MySqlDB
from db.mypartitionedtable import MyPartitionedTable

class TestMyPartitionedTable(unittest.TestCase):
    def test_mypartition(self):
        db = MySqlDB()

        db.dropTable("test_ptable1")
        pt = MyPartitionedTable("test_ptable1", "id")

        p1 = pt.getPartitionName(1)
        self.assertFalse(pt.partitionExists(p1))

        pt.addPartition("1")
        self.assertTrue(pt.partitionExists(p1))

        sql = "insert into test_ptable1 values(1, 'test1');"
        db.execSql(sql)

        cnt = db.countTable("test_ptable1")
        self.assertEqual(cnt, 1)

        p2 = pt.getPartitionName(2)
        pt.addPartition(2)
        
        self.assertTrue(pt.partitionExists(p2))

        sql = "insert into test_ptable1 values(2, 'test2');"
        db.execSql(sql)
        
        cnt = db.countTable("test_ptable1")
        self.assertEqual(cnt, 2)

        db.dropTable("test_ptable1")


if __name__ == "__main__":
    unittest.main()
