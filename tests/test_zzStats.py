import __init__

import unittest
from strategy.ZzStatsStrategy import ZzStatsStrategy
import lib
import lib.tradelib as tradelib
import lib.backtests as backtests
from consts import *
from db.mysql import MySqlDB

trade_name = "test6"   
"""
SELECT year(open_datetime) y, month(open_datetime) m, 
sum(profit) p, count(profit) c
FROM `trades`
where trade_name = 'test6'
group by y, m;

select sum(profit) total_profit, 
sum(if(profit>0,1,0)) wins, sum(if(profit<0,1,0)) loses 
from trades
where trade_name = 'test6';
"""

def run_backtest(trade_name, st, ed, os, buy_budget, sell_budget, profit):
    granularity = "D"
    config = {"granularity": granularity, 
        "classifier": "kmpeak_backtest",
        "min_profit": profit,
        "max_fund": 200000,
        "trade_mode": TRADE_MODE_BOTH,
        "data_dir": "app/stockAnalyzer/1",
        "obsyear": 2020,
        "startep": st,
        "endep": ed,
        "use_master": True
    }
    
    #config["codenames"] = []
    sql = """SELECT distinct i.codename FROM stockanalyzer.anal_zzitems_D_5_5 i
left join stockanalyzer.anal_zzkmgroups_D_5_5 g on i.zzitemid = g.zzitemid
where g.km_setid = 1
and g.km_id in
('pek212212_0000000017','pek212112_0000000023','pek212122_0000000005','pek212112_0000000007','pek212222_0000000013','pek212122_0000000007',
'pek212111_0000000004','pek121111_0000000031','pek121211_0000000001','pek121211_0000000021','pek121121_0000000018','pek211212_0000000000',
'pek212212_0000000009','pek121222_0000000012','pek121111_0000000017')
and i.codename in 
(select distinct codename 
    FROM stockanalyzer.anal_zzcode_D c
    where obsyear = 2020 and min_nth_volume >= 100000)
order by rand()
limit 10
;"""
    codenames = []
    for (codename,) in MySqlDB().execSql(sql):
        codenames.append(codename)
    
    
    #codenames = ['8715.T','1720.T','6473.T','8359.T','6997.T','6993.T','2586.T','6058.T','2002.T','1407.T','4436.T','9042.T','2587.T','3665.T','8616.T','9119.T',
    #'6902.T','4324.T','7747.T','3661.T','6754.T','7951.T','6544.T','8005.T','6981.T','2501.T','5929.T','4599.T','4921.T','8233.T','8016.T','6740.T','9107.T','6481.T',
    #'6755.T','7186.T','6592.T','3407.T','4042.T','2267.T','6586.T','9468.T','6701.T','5721.T','2489.T','2337.T','4568.T','9719.T','4062.T','9513.T','8304.T','6268.T',
    #'4704.T','8088.T','5631.T','5938.T','5110.T','9501.T','4151.T','2440.T','9404.T','6871.T','9424.T','2315.T','9735.T','3088.T','9505.T','7261.T','7201.T','7545.T',
    #'5706.T','2914.T','6632.T','6383.T','7276.T','3635.T','3659.T','4974.T','8830.T','6305.T','9301.T','9399.T','9502.T','2229.T','8306.T','4714.T','2371.T','9684.T',
    #'6804.T','3048.T','1973.T','8804.T','3865.T','8056.T','7984.T','7167.T','8918.T','9603.T','9532.T','7616.T',]
    
    #codenames = ["1911.T"]
    config["codenames"] = codenames

    
    
    #config["codenames"] = ['9997.T']
    #config["codenames"] = []
    
    config["market"] = "prime"
    strategy = ZzStatsStrategy(config)
    # name, interval, startep, endep, strategy, orderstopep=0, buy_budget=1000000, sell_budget=1000000
    report = backtests.runBacktest(trade_name, granularity, st, ed, strategy, orderstopep=os,
            buy_budget=buy_budget, sell_budget=buy_budget)
    return report


class TestZzStatsStrategy(unittest.TestCase):
    def testCase1(self):
        st = lib.str2epoch("2021-01-05T00:00:00") 
        ed = lib.str2epoch("2022-12-01T00:00:00")
        os = lib.str2epoch("2022-11-20T00:00:00")
        # name, st, ed, os, buy_budget, sell_budget, profit
        report = run_backtest(trade_name, st, ed, os, 1000000, 1000000, 10000)
        #self.assertEqual(report["trade_count"],30)
        #self.assertEqual(report["codename"],"^N225")
        #self.assertGreater(report["sell_offline"], 0)

        #print(report)
        #import json
        #print(json.dumps(tm.portforio.history, indent=4))

        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()