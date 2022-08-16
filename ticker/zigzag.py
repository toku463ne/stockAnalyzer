from ticker import Ticker
import lib.indicators as libind

class Zigzag(Ticker):
    def initData(self, ohlcv, size=5):
        self.pos = 0
        (ep, dt, _, h, l, _, _) = ohlcv
        (self.ep, 
        self.dt, 
        self.dirs, 
        self.prices, _) = libind.zigzag(ep, dt, h, l, size)


    def getData(self, i, n=1):
        if n == 1 and i >= 0:
            return (self.ep[i], self.dt[i], self.dirs[i], self.prices[i])
        else:
            j = i-n+1
            if j < 0:
                j = 0
            i = i+1
            return (self.ep[j:i], self.dt[j:i], self.dirs[j:i], self.prices[j:i])



    