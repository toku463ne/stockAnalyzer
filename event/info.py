'''
Created on 2019/05/23

@author: kot
'''

class InfoEvent(object):
    def __init__(self, ep, price, inf, marker="*",color="k"):
        self.ep = ep
        self.price = price
        self.marker = marker
        self.info = inf
        self.color = color
