import lib

class Predictor(object):
    def initAttrFromArgs(self, args, name, default=None):
        lib.initAttrFromArgs(self, args, name, default=default)
        
    """
    Return pandas DataFrame with headers
    codename, x, y
    """
    def predict(self, epoch):
        pass
