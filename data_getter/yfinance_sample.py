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
"""
{'zip': '105-7317',
 'sector': 'Communication Services',
 'fullTimeEmployees': 58786,
 'longBusinessSummary': 'SoftBank Group Corp. provides telecommunication services in Japan and internationally. The company operates through four segments: Investment Business of Holding Companies; SoftBank Vision Fund and Other SBIA-Managed Funds; SoftBank; and Arm. It offers mobile communications, broadband, and fixed-line communications services; and sells mobile devices. The company is also involved in internet advertising marketing, online advertising distribution, e-book distribution, investment, and planning and operation of a fashion e-commerce website; and designs and develops mobile robots. In addition, it designs microprocessor intellectual property and related technology; sells software tools; and generates, supplies, and sells electricity from renewable energy sources, as well as offers related services. Further, the company provides investment management and marketing services, smartphone payment services, PC software downloads, banking services, and solutions and services for online businesses; distributes video, voice, and data content; manufactures, distributes, and sells IT-related products, as well as IT-related services; and manages funds. The company operates a professional baseball team, as well as manages and maintains baseball stadium and other sports facilities; and operates ITmedia, an IT information site. It also operates the fashion online shopping website ZOZOTOWN. The company was formerly known as SoftBank Corp. and changed its name to SoftBank Group Corp. in July 2015. SoftBank Group Corp. was incorporated in 1981 and is headquartered in Tokyo, Japan.',
 'city': 'Tokyo',
 'phone': '81 3 6889 2000',
 'country': 'Japan',
 'companyOfficers': [],
 'website': 'https://group.softbank',
 'maxAge': 1,
 'address1': '1-7-1, Kaigan',
 'industry': 'Telecom Services',
 'address2': 'Minato-ku',
 'ebitdaMargins': 0.25201,
 'profitMargins': 0.57988,
 'grossMargins': 0.51665,
 'operatingCashflow': 2103621976064,
 'revenueGrowth': 0.114,
 'operatingMargins': 0.10773,
 'ebitda': 1507280945152,
 'targetLowPrice': 7400,
 'recommendationKey': 'buy',
 'grossProfits': 2874929000000,
 'freeCashflow': 325206376448,
 'targetMedianPrice': 10000,
 'currentPrice': 5481,
 'earningsGrowth': None,
 'currentRatio': 0.802,
 'returnOnAssets': 0.00984,
 'numberOfAnalystOpinions': 15,
 'targetMeanPrice': 9808.7,
 'debtToEquity': 170.992,
 'returnOnEquity': 0.36495,
 'targetHighPrice': 12600,
 'totalCash': 5548188106752,
 'totalDebt': 21099454660608,
 'totalRevenue': 5981139894272,
 'totalCashPerShare': 3237.707,
 'financialCurrency': 'JPY',
 'revenuePerShare': 3381.237,
 'quickRatio': 0.675,
 'recommendationMean': 1.8,
 'exchange': 'JPX',
 'shortName': 'SOFTBANK GROUP CORP',
 'longName': 'SoftBank Group Corp.',
 'exchangeTimezoneName': 'Asia/Tokyo',
 'exchangeTimezoneShortName': 'JST',
 'isEsgPopulated': False,
 'gmtOffSetMilliseconds': '32400000',
 'quoteType': 'EQUITY',
 'symbol': '9984.T',
 'messageBoardId': 'finmb_23013',
 'market': 'jp_market',
 'annualHoldingsTurnover': None,
 'enterpriseToRevenue': 4.516,
 'beta3Year': None,
 'enterpriseToEbitda': 17.921,
 '52WeekChange': -0.3543409,
 'morningStarRiskRating': None,
 'forwardEps': 793.41,
 'revenueQuarterlyGrowth': None,
 'sharesOutstanding': 1709660032,
 'fundInceptionDate': None,
 'annualReportExpenseRatio': None,
 'totalAssets': None,
 'bookValue': 6155.731,
 'sharesShort': None,
 'sharesPercentSharesOut': None,
 'fundFamily': None,
 'lastFiscalYearEnd': 1617148800,
 'heldPercentInstitutions': 0.37238997,
 'netIncomeToCommon': 3437314965504,
 'trailingEps': 1740.613,
 'lastDividendValue': 22,
 'SandP52WeekChange': 0.26353753,
 'priceToBook': 0.8903898,
 'heldPercentInsiders': 0.30174,
 'nextFiscalYearEnd': 1680220800,
 'yield': None,
 'mostRecentQuarter': 1632960000,
 'shortRatio': None,
 'sharesShortPreviousMonthDate': None,
 'floatShares': 1199480050,
 'beta': 1.199778,
 'enterpriseValue': 27012110483456,
 'priceHint': 2,
 'threeYearAverageReturn': None,
 'lastSplitDate': 1561420800,
 'lastSplitFactor': '2:1',
 'legalType': None,
 'lastDividendDate': 1648598400,
 'morningStarOverallRating': None,
 'earningsQuarterlyGrowth': None,
 'priceToSalesTrailing12Months': 1.5666991,
 'dateShortInterest': None,
 'pegRatio': -0.34,
 'ytdReturn': None,
 'forwardPE': 6.9081564,
 'lastCapGain': None,
 'shortPercentOfFloat': None,
 'sharesShortPriorMonth': None,
 'impliedSharesOutstanding': None,
 'category': None,
 'fiveYearAverageReturn': None,
 'previousClose': 5643,
 'regularMarketOpen': 5550,
 'twoHundredDayAverage': 7580.54,
 'trailingAnnualDividendYield': 0.007797271,
 'payoutRatio': 0.0256,
 'volume24Hr': None,
 'regularMarketDayHigh': 5578,
 'navPrice': None,
 'averageDailyVolume10Day': 19897080,
 'regularMarketPreviousClose': 5643,
 'fiftyDayAverage': 6246.38,
 'trailingAnnualDividendRate': 44,
 'open': 5550,
 'toCurrency': None,
 'averageVolume10days': 19897080,
 'expireDate': None,
 'algorithm': None,
 'dividendRate': 44,
 'exDividendDate': 1648598400,
 'circulatingSupply': None,
 'startDate': None,
 'regularMarketDayLow': 5476,
 'currency': 'JPY',
 'trailingPE': 3.1488905,
 'regularMarketVolume': 14780400,
 'lastMarket': None,
 'maxSupply': None,
 'openInterest': None,
 'marketCap': 9370646609920,
 'volumeAllCurrencies': None,
 'strikePrice': None,
 'averageVolume': 18682101,
 'dayLow': 5476,
 'ask': 5486,
 'askSize': 0,
 'volume': 14780400,
 'fiftyTwoWeekHigh': 10695,
 'fromCurrency': None,
 'fiveYearAvgDividendYield': 0.57,
 'fiftyTwoWeekLow': 5057,
 'bid': 5477,
 'tradeable': False,
 'dividendYield': 0.0078,
 'bidSize': 0,
 'dayHigh': 5578,
 'regularMarketPrice': 5481,
 'preMarketPrice': None,
 'logo_url': 'https://logo.clearbit.com/group.softbank'}
"""
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
