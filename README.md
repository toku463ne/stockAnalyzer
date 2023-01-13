# stockAnalyzer
analyze stock prices

# prepare mysql database and accounts
```
CREATE DATABASE stockanalyzer CHARACTER SET 'utf8';
CREATE DATABASE stockanalyzer_test CHARACTER SET 'utf8';
CREATE USER IF NOT EXISTS 'stockuser'@'localhost' IDENTIFIED BY 'stockpass';
GRANT ALL PRIVILEGES ON stockanalyzer.* to 'stockuser'@'localhost';
GRANT ALL PRIVILEGES ON stockanalyzer_test.* to 'stockuser'@'localhost';
```
# Install MySQL tremmean
https://github.com/StirlingMarketingGroup/mysql-trimmean  


# install python libraries
pip3 install pymysql yfinance pandas_datareader pandas numpy sqlalchemy falcon xlrd path sklearn matplotlib jupyter


# jupyter notebook
jupyter notebook --generate-config
jupyter notebook password
jupyter notebook
# access to http://localhost:8888

# chart amChart
https://qiita.com/siruku6/items/09d588329a8e4564d9f5
npm install express
npm install @amcharts/amcharts5


# API


# fast
https://kunai-lab.hatenablog.jp/entry/2018/04/08/134924


# codes
nikkei 225 : ^N225
softbank: 9984.T
