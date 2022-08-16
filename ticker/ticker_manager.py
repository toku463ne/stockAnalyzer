import ticker
from ticker.tick import Ticker

class TickerManager(object):
    def __init__(self, codenames, granularity):
        self.granularity = granularity
        self.codenames = codenames
        self.tickers = {}

    def loadTickers(self, startep, endep):
        for instrument in self.codenames:
            self.tickers[instrument] = Ticker(instrument, self.granularity, startep, endep)

    def tick(self, instrument, ep=0):
        return self.tickers[instrument].tick(ep)