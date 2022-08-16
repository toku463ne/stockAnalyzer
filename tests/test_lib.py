import __init__
import unittest
import lib

class TestLib(unittest.TestCase):
    def test_mergeJson(self):
        j1 = {"a": 1, "b": 2}
        j2 = {"b": 3}
        lib.mergeJson(j1, j2)
        self.assertEqual(j1["a"], 1)
        self.assertEqual(j1["b"], 3)

        j1 = {"a": 1, "b": {"c": 2}}
        j2 = {"b": {"c": 3}}
        lib.mergeJson(j1, j2)
        self.assertEqual(j1["a"], 1)
        self.assertEqual(j1["b"]["c"], 3)

        j1 = {"a": 1, "b": {"c": 2}}
        j2 = {"b": {"c": [3,4,5,6]}}
        lib.mergeJson(j1, j2)
        self.assertEqual(j1["a"], 1)
        self.assertEqual(j1["b"]["c"], [3,4,5,6])

        j1 = {"a": 1, "b": {"c": 2}}
        j2 = {"b": 2}
        lib.mergeJson(j1, j2)
        self.assertEqual(j1["a"], 1)
        self.assertEqual(j1["b"], 2)



if __name__ == "__main__":
    unittest.main()
