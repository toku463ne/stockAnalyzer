'''
Created on 2019/04/21

@author: kot
'''
from consts import *

class SignalEvent(object):
    def __init__(self, _id, signal):
        self.id = _id
        self.type = EVETYPE_SIGNAL
        self.signal = signal
        