import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from env import *
from consts import *

import data_getter
from analyzer import Analyzer
from ticker.zigzag import Zigzag
from db.mysql import MySqlDB
import db.mydf as mydf
import lib
import lib.tradelib as tradelib
import lib.naming as naming

from sklearn.cluster import KMeans
import pandas as pd
import numpy as np
import json
import matplotlib.pyplot as plt
import math
import pickle
from datetime import datetime


class ZzAnalyzer(Analyzer):
    def __init__(self, config, use_master=False):
        self.initAttrFromArgs(config, "granularity", "D")
        self.initAttrFromArgs(config, "n_points", 5)
        self.initAttrFromArgs(config, "zz_size", 5)
        self.initAttrFromArgs(config, "zz_middle_size", 2)
        self.initAttrFromArgs(config, "km_granularity", "Y")
        self.initAttrFromArgs(config, "km_avg_size", 10)
        self.initAttrFromArgs(config, "km_max_k", 8)
        self.initAttrFromArgs(config, "km_min_size", 100)
        self.initAttrFromArgs(config, "km_max_size", 1000)
        self.initAttrFromArgs(config, "codenames", [])
        self.initAttrFromArgs(config, "deflect_rate", 0.65)
        self.initAttrFromArgs(config, "min_cnt_a_year", 10)
        self.initAttrFromArgs(config, "km_setname", "")
        self.initAttrFromArgs(config, "n_candles", 3)