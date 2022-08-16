from audioop import reverse
import __init__

import tools.jpx as jpx
from db.mydf import MyDf
from db.mysql import MySqlDB
import pandas as pd
import json

def initTables():
    db = MySqlDB()
    table_names = ["jpx_raw"]
    for table_name in table_names:
        db.dropTable(table_name)

    table_names = ["group_members", 
                    "igroups", 
                    "industry", 
                    "anal_zzdata",
                    "anal_zzgroups",
                    "codes"]
    for table_name in table_names:
        db.dropTable(table_name)

    table_names.reverse()
    for table_name in table_names:
        db.createTable(table_name)

    for table_name in ["jpx_33gyoshu", 
                        "jpx_17gyoshu", 
                        "jpx_kibo"]:
        db.dropTable(table_name)
        db.createTable(table_name, "codes")

    db.createTable("codes", "codes")
    prepareJPXTables()


def prepareJPXTables():
    mydf = MyDf()
    db = MySqlDB()
    df = jpx.getJPXDf()
    df.to_sql("jpx_raw", con=mydf.getEngine(), if_exists="replace", index=False)

    #sql = "select distinct 33業種コード as codename, 33業種区分 as name, 'jpx' as source from jpx_raw where 33業種コード != '-';"
    #df = pd.read_sql_query(sql=sql, con=mydf.getEngine())
    #df.to_sql("jpx_33gyoshu", con=mydf.getEngine(), if_exists="append", index=False)
    
    #sql = "select distinct 17業種コード as codename, 17業種区分 as name, 'jpx' as source from jpx_raw where 17業種コード != '-';"
    #df = pd.read_sql_query(sql=sql, con=mydf.getEngine())
    #df.to_sql("jpx_17gyoshu", con=mydf.getEngine(), if_exists="append", index=False)
    
    #sql = "select distinct 規模コード as codename, 規模区分 as name, 'jpx' as source from jpx_raw where 規模コード != '-';"
    #df = pd.read_sql_query(sql=sql, con=mydf.getEngine())
    #df.to_sql("jpx_kibo", con=mydf.getEngine(), if_exists="append", index=False)
    

    sql = "delete from codes where source = 'jpx';"
    db.execSql(sql)

    sql = """insert into codes(codename, name, source, market, market_detail, market_type)
select distinct concat(コード, '.T') as codename, 
銘柄名 as name, 
'jpx' as source, 
case 
    when 市場・商品区分 = 'ETF・ETN' then 'etf'
    when 市場・商品区分 = 'PRO Market' then 'pro'
    when 市場・商品区分 = 'REIT・ベンチャーファンド・カントリーファンド・インフラファンド' then 'reit'
    when 市場・商品区分 = 'グロース（内国株式）' then 'growth'
    when 市場・商品区分 = 'グロース（外国株式）' then 'growth'
    when 市場・商品区分 = 'スタンダード（内国株式）' then 'standard'
    when 市場・商品区分 = 'スタンダード（外国株式）' then 'standard'
    when 市場・商品区分 = 'プライム（内国株式）' then 'prime'
    when 市場・商品区分 = 'プライム（外国株式）' then 'prime'
    when 市場・商品区分 = '出資証券' then 'shoken'
    else ''
end as market,
市場・商品区分 as market_detail, 
case 
    when 市場・商品区分 = 'グロース（内国株式）' then 'domestic'
    when 市場・商品区分 = 'グロース（外国株式）' then 'overseas'
    when 市場・商品区分 = 'スタンダード（内国株式）' then 'domestic'
    when 市場・商品区分 = 'スタンダード（外国株式）' then 'overseas'
    when 市場・商品区分 = 'プライム（内国株式）' then 'domestic'
    when 市場・商品区分 = 'プライム（外国株式）' then 'overseas'
    else 'other'
end as market_type
from jpx_raw;"""
    db.execSql(sql)
    
    sql = "delete from codes where source = 'manual';"
    db.execSql(sql)

    with open("additional_codes.json", "r") as f:
        data = json.load(f)
        for code in data.keys():
            cols = ["codename", "source"]
            vals = ["'%s'" % code, "'manual'"]
            if "name" in data[code].keys():
                cols.append("name")
                vals.append("'%s'" % data[code]["name"])
            if "market" in data[code].keys():
                cols.append("market")
                vals.append("'%s'" % data[code]["market"])
            if "market_type" in data[code].keys():
                cols.append("market_type")
                vals.append("'%s'" % data[code]["market_type"])
            
            sql = "insert into %s(%s) values(%s);" % ("codes", ",".join(cols), ",".join(vals))
            db.execSql(sql)
            


if __name__ == "__main__":
    initTables()
    #prepareJPXTables()