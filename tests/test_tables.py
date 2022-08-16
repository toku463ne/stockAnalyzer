import __init__
import unittest
import db.inittables as t

class TestTables(unittest.TestCase):
    def test_prepareTable(self):
        t.initTables()

if __name__ == "__main__":
    unittest.main()
