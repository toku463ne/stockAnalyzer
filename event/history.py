from consts import *
import lib


class HistEvent(object):
    def __init__(self, epoch, price, 
                 index_id, index_type, desc="", 
                 data={}, marker="x", color="k"):
        self.epoch = epoch
        self.price = price
        self.index_type = index_type
        self.index_id = index_id
        self.pos = 0
        self.desc = desc
        self.data = data
        self.marker = marker
        self.color = color
        
    def setPos(self, pos):
        self.pos = pos
        
    def log(self, ep):
        lt = ""
        index_type = self.index_type
        if index_type == LINE_TPTREND:
            lt = "tp"
        elif index_type == LINE_BTTREND:
            lt = "bt"
        elif index_type == LINE_MAX:
            lt = "ma"
        elif index_type == LINE_MIN:
            lt = "mi"
        ps = ""
        pos = self.pos
        if pos == LINEDIST_ABOVE_FAR:
            ps = "ab_far"
        elif pos == LINEDIST_ABOVE_NEAR:
            ps = "ab_nea"
        elif pos == LINEDIST_ABOVE_TOUCHED:
            ps = "ab_tou"
        elif pos == LINEDIST_CROSSED:
            ps = "cross"
        elif pos == LINEDIST_BELOW_FAR:
            ps = "bl_far"
        elif pos == LINEDIST_BELOW_NEAR:
            ps = "bl_nea"
        elif pos == LINEDIST_BELOW_TOUCHED:
            ps = "bl_tou"
        lib.printDebug(ep, "id=%s type=%s pos=%s" % (
            self.index_id, lt, ps))
