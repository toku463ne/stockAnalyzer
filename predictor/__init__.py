import lib

class Predictor(object):
    def initAttrFromArgs(self, args, name, default=None):
        if name in args.keys():
            setattr(self, name, args[name])
        elif default is None:
            raise Exception("%s is necessary!" % name)
        else:
            setattr(self, name, default)
        
    """
    Return pandas DataFrame with headers
    codename, x, y
    """
    def predict(self, epoch):
        pass
