import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

_BASE_URL_ = 'https://query2.finance.yahoo.com'
_SCRAPE_URL_ = 'https://finance.yahoo.com/quote'
# https://aroussi.com/post/python-yahoo-finance

#from pandas_datareader import data as pdr

#import yfinance as yf
#yf.pdr_override() # <== that's all it takes :-)

# download dataframe using pandas_datareader
#data = pdr.get_data_yahoo("SPY", start="2017-01-01", end="2017-04-30")
# df.to_sql(con=con, name='table_name_for_df', if_exists='replace', flavor='mysql')

import yfinance as yf

msft = yf.Ticker("MSFT")

# get stock info
msft.info

# get historical market data
hist = msft.history(period="max")
# period: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max
# interval: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo
# start: YYYY-MM-DD
# end: YYYY-MM-DD 
'''
def history(self, period="1mo", interval="1d",
                start=None, end=None, prepost=False,
                actions=True, auto_adjust=True, proxy=None,
                threads=True, group_by='column', progress=True,
                timeout=None, **kwargs):
'''

# show actions (dividends, splits)
msft.actions

# show dividends
msft.dividends

# show splits
msft.splits

# show financials
msft.financials
msft.quarterly_financials

# show major holders
msft.major_holders

# show institutional holders
msft.institutional_holders

# show balance sheet
msft.balance_sheet
msft.quarterly_balance_sheet

# show cashflow
msft.cashflow
msft.quarterly_cashflow

# show earnings
msft.earnings
msft.quarterly_earnings

# show sustainability
msft.sustainability

# show analysts recommendations
msft.recommendations

# show next event (earnings, etc)
msft.calendar

# show ISIN code - *experimental*
# ISIN = International Securities Identification Number
msft.isin

# show options expirations
msft.options

# show news
msft.news

# get option chain for specific expiration
opt = msft.option_chain('YYYY-MM-DD')
# data available via: opt.calls, opt.puts
