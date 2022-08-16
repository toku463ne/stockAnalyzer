import __init__
import unittest
import tools.jpx as jpx

class TestJPX(unittest.TestCase):
    def test_jpx(self):
        df = jpx.getJPXDf()

        self.assertGreater(len(df), 0)
        #print(p)

if __name__ == "__main__":
    unittest.main()
