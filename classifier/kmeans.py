import sys
import os
import math
import pandas as pd
import pickle
from sklearn.cluster import KMeans
from consts import *


sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from classifier import Classifier
import lib
import lib.naming as naming
from env import *


class KmClassifier(Classifier):
    def __init__(self, config):
        super(KmClassifier, self).__init__(config)
        self.initAttrFromArgs(config, "data_dir", "")
        self.initAttrFromArgs(config, "km_max_retries", 5)
        self.initAttrFromArgs(config, "enough_score", 0.7)
        self.initAttrFromArgs(config, "min_km_size", 100)
        self.initAttrFromArgs(config, "use_master", False)

        self.kmdata_dir = lib.ensureDataDir(self.data_dir, subdir=self.classifier)
        self.ensureTable("kmItemsTable", "clf_kmitems", "clf_kmitems")
        self.ensureTable("kmInfoTable", "clf_kminfo")

        self.type_name = ""

        self.kms = {}


    # return (k, finish_now)
    def getNextK(self, k, score, max_score, k_limit, k_step_size):
        return (2, False)

    # df and km_vals must have same number or rows
    # df will be used to evaluate the kmeans results
    def calcScore(self, df, km_vals):
        pass

    def calcStats(self):
        pass

    def testKm(self):
        pass
    
    
    def loadKmModel(self):
        # to load
        kms = {}
        sql = "select distinct km_id from clf_kminfo where clf_id=%d;" % (self.id)
        for (km_id,) in self.redb.execSql(sql):
            (km_name, km_groupid) = naming.extendKmId(km_id)
            kmf = self.getPklfilePath(km_name)
            if os.path.exists(kmf):
                with open(kmf, "rb") as f:
                    kms[km_name] = pickle.load(f)
        self.kms = kms


    def getPklfilePath(self, km_name):
        d = "%s/%s.pkl" % (self.kmdata_dir, km_name)
        return d

    # df 
    # feed_vals 
    def execKmeans(self, df, feed_vals):
        item_ids = df.index.tolist()
        clf_id = self.id
        enough_score = self.enough_score
        log("starting kmeans calculation")

        km_groups = {}
        item_groups = {}
        kms = {}
        #vals = df.values
        for i in range(len(feed_vals)):
            v = feed_vals[i]
            km_name = self.getClassId(v)
            if km_name not in km_groups.keys():
                km_groups[km_name] = []
                item_groups[km_name] = []
            km_groups[km_name].append(v)
            item_groups[km_name].append(item_ids[i])
            
        for km_name in km_groups.keys():
            log("calculating %s" % (km_name))
            max_score = 0
            max_scores = []
            max_expects = []
            max_cnts = []
            max_n = 0
            k = 1
            max_k = 1
            score = 0
            km_try_cnt = 0
            vals = km_groups[km_name]
            item_ids = item_groups[km_name]
            km_predicted = [0]*len(item_ids)
            scores = []
            expects = []
            while km_try_cnt < self.km_max_retries:
                km_try_cnt += 1
                n = len(vals)
                k_limit = math.floor(n/self.min_km_size)
                k_step_size = math.ceil(k_limit/self.km_max_retries)
                if k > 1:
                    km = KMeans(n_clusters=k, n_init=10)
                    km = km.fit(vals)
                    km_vals = km.fit_predict(vals)
                    subdf = df.filter(items=item_ids, axis=0)
                    subdf["km_val"] = km_vals
                    score, scores, expects, cnts = self.calcScore(subdf, k)
                    if score > max_score:
                        kms[km_name] = km
                        max_k = k
                        max_scores = scores
                        max_expects = expects
                        max_cnts = cnts
                        max_n = n

                    # save kmeans model
                    if score > max_score:
                        pklfile = self.getPklfilePath(km_name)
                        with open(pklfile, "wb") as f:
                            pickle.dump(km, f)   
                        km_predicted = km_vals                 

                else:
                    max_score, max_scores, max_expects, max_cnts = self.calcScore(df.filter(items=item_ids, axis=0), 1)
                    #kmg = [0]*len(vals)
                    kms[km_name] = None
                    max_n = n
                    
                (k, finish_now) = self.getNextK(k, score, max_score, k_limit, k_step_size)
                if finish_now:
                    break

            km_ids = [""]*max_k
            for j in range(max_k):
                km_id = naming.getKmId(km_name, j)
                km_ids[j] = km_id
                sql = """replace into clf_kminfo(clf_id, km_id, score, expected, cnt)
values('%s', '%s', %f, %f, %d);""" % (clf_id, km_id, max_scores[j], max_expects[j], max_cnts[j])
                self.updb.execSql(sql)
            
            g_item_ids = item_groups[km_name]
            for l in range(len(g_item_ids)):
                item_id = g_item_ids[l]
                km_id = km_ids[km_predicted[l]]
                sql = """replace into clf_kmitems(item_id, clf_id, km_id)
values(%d, %d, '%s')""" % (item_id, clf_id, km_id)
                self.updb.execSql(sql)

        self.kms = kms
        log("completed kmeans calculation")


    def predict(self, vals):
        km_name = self.getClassId(vals)
        if km_name in self.kms.keys():
            km_groupid = self.kms[km_name].predict([vals])[0]
        else:
            km_groupid = 0
        km_id = naming.getKmId(km_name, km_groupid)
        return km_id       


        
