from ticker import Ticker
import lib.indicators as libind


class SMA(Ticker):
    def initData(self, ohlcv, span=20):
        (ep, dt, _, _, _, p, _) = ohlcv
        x, start_i = libind.sma(p, span)
        self.p = x
        self.ep = ep[start_i:]
        self.dt = dt[start_i:]
        self.span = span
        
    def getData(self, i, n=0):
        if n == 1 and i >= 0:
            return (self.ep[i], self.dt[i], self.p[i])
        elif i >= 0:
            j = i-n+1
            i = i+1
            return (self.ep[j:i], self.dt[j:i], self.p[j:i])
        else:
            return (0,None, 0)