import __init__

import unittest
from strategy.ZzStatsStrategy import ZzStatsStrategy
import lib
import lib.tradelib as tradelib
from time_ticker import TimeTicker
from executor import Executor
from trade_manager import TradeManager
from portforio import Portoforio

class TestZzStatsStrategy(unittest.TestCase):

    def _run(self, st, ed, os, profit):
        granularity = "D"
        args = {"codenames": ["^N225", "1332.T", "1379.T", 
        "1716.T", "1720.T", "1973.T", "2121.T", "2160.T",
        "2695.T", "2923.T", "3001.T",
        "3092.T", "3197.T", 
        "4080.T", "4337.T", 
        "5020.T", "5108.T", 
        "6042.T", "6360.T", "7014.T", "7150.T", "7157.T",
        "7175.T", "7203.T", "8001.T", "8002.T", "8244.T",
        "8306.T", "8308.T", 
        "9005.T", "9066.T", ], 
            "granularity": granularity, 
            "min_profit": profit,
            "min_km_count": 5,
            "max_std": 0.3,
            "max_fund": 10000000,
            "kmpkl_file": "/home/administrator/stockanaldata/zz/km_2015-2020.pkl"}
        #    "kmpkl_file": "tests/test_zzStats.d/km_2015-2021.pkl"}
        
        #args["codenames"] = []
        args["codenames"] = ['1928.T','2695.T','4613.T','4819.T','4917.T','5232.T','5563.T',
        '5727.T','6988.T','7618.T','8133.T','8219.T','2752.T','8252.T','9045.T','9202.T',
        '9504.T','2269.T','2802.T','3231.T','4187.T','6954.T','7278.T','3028.T','7911.T',
        '9533.T','2120.T','2749.T','3328.T','4373.T','4681.T','7201.T','7730.T','7956.T',
        '3036.T','8473.T','9757.T','1911.T','2331.T','2412.T','2685.T','3134.T','3762.T',
        '3902.T','3925.T','3086.T','4063.T','4186.T','4739.T','4776.T','5393.T','6101.T',
        '6569.T','6572.T','6616.T','6728.T','3087.T','6871.T','7453.T','7518.T','8016.T',
        '8053.T','3681.T','4047.T','6093.T','7593.T','3182.T','8203.T','8253.T','3382.T',
        '3543.T','4251.T','4722.T','5020.T','5121.T','5411.T','5901.T','3391.T','5991.T',
        '6178.T','7224.T','3962.T','4443.T','3665.T','4552.T','4768.T','6191.T','7085.T',
        '9001.T','9519.T','9719.T','3482.T','3678.T','3923.T','7816.T','2875.T','4516.T',
        '6370.T','2811.T','3465.T','1945.T','3565.T','9042.T','9433.T','3288.T','4228.T',
        '4536.T','5802.T','3769.T','6113.T','7004.T','7282.T','7735.T','7966.T','8101.T',
        '8306.T','4661.T','3978.T','8802.T','2726.T','4680.T','4714.T','6406.T','6474.T',
        '9616.T','2607.T','4480.T','5929.T','5949.T','6498.T','9401.T','3563.T','4403.T',
        '3661.T','4203.T','7187.T','7990.T','8309.T','9107.T','3099.T','4449.T','6055.T',
        '6544.T','9505.T','9997.T','8919.T','2489.T','3697.T','4502.T','6787.T','6857.T',
        '6963.T','9880.T','2372.T','3655.T','4631.T','5985.T','6464.T','6810.T','9787.T',
        '2768.T','2980.T','6134.T','6615.T','7414.T','2929.T','5074.T','6361.T','6640.T',
        '6997.T','7433.T','3675.T','6533.T','8136.T','3053.T','7611.T','8035.T','8381.T',
        '7936.T','8935.T','2427.T','4927.T','3034.T','3132.T','6383.T','6902.T','2281.T',
        '8624.T','4587.T','5019.T','7976.T','6310.T','7182.T','9449.T','9513.T','7868.T',
        '5076.T','9613.T','2492.T','6794.T','4689.T','4088.T','7575.T','5301.T','4446.T',
        '6070.T','5491.T','5809.T','6062.T','6071.T','2432.T','6088.T','6095.T','6330.T',
        '6440.T','6532.T','6584.T','6703.T','6755.T','6925.T','2440.T','7012.T','7199.T',
        '7203.T','7220.T','7247.T','7269.T','7272.T','7296.T','2462.T','7780.T','7970.T',
        '8097.T','8544.T','8600.T','9142.T','9364.T','9409.T','9412.T','9962.T','2433.T',
        '2531.T','4348.T','6727.T','2502.T','6923.T','7148.T','7476.T','8830.T','9603.T',
        '4676.T','6849.T','7915.T','9684.T','1976.T','6630.T','7545.T','1944.T']
        #args["codenames"] = ["1928.T"]
        
        args["market"] = "prime"
        strategy = ZzStatsStrategy(args, use_master=True, load_zzdb=True)
        ticker = TimeTicker(tradelib.getUnitSecs(granularity), st, ed)
        executor = Executor()
        portforio = Portoforio("zzstats_test4")
        tm = TradeManager("zigzag stats strategy", ticker, strategy, executor, portforio)
        report = tm.run(orderstopep=os)

        return report


    def testCase1(self):
        st = lib.str2epoch("2022-01-01T00:00:00") 
        ed = lib.str2epoch("2022-12-01T00:00:00")
        os = lib.str2epoch("2022-11-20T00:00:00")
        report = self._run(st, ed, os, 10000)
        self.assertEqual(report["trade_count"],1)
        self.assertEqual(report["codename"],"^N225")
        self.assertGreater(report["sell_offline"], 0)

        #print(report)
        #import json
        #print(json.dumps(tm.portforio.history, indent=4))

        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()