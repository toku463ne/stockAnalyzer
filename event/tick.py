from consts import *

class TickEvent(object):
    def __init__(self, epoch, price, o=0, h=0, l=0, c=0, v=0):
        self.type = EVETYPE_TICK
        self.time = epoch
        self.price = price
        self.o = o
        self.h = h
        self.l = l
        self.c = c
        self.v = v
        

        
        