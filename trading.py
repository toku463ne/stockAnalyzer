import time
from consts import *

def run(timeTicker, manager, strategy, flushHist=False):
    order_events = []

    cnt = 0
    while timeTicker.tick():
        epoch = timeTicker.epoch

        while len(order_events) > 0:
            manager.receiveOrder(order_events.pop(0))

        signal_events = manager.onTick(epoch)
        while len(signal_events) > 0:
            strategy.onSignal(signal_events.pop(0))
        
        order_events = strategy.onTick(epoch)
        
        cnt += 1
        if cnt >= TRADING_HISTORY_FLUSH_INTERVAL:
            cnt = 0
            if flushHist:
                manager.flushHistory()
        

        