
class TimeTicker(object):
    def __init__(self, interval, startep, endep):
        self.interval = interval
        self.startep = startep
        self.endep = endep
        self.epoch = startep
        self.EOF = False

    def tick(self):
        self.epoch += self.interval
        if self.epoch > self.endep:
            self.EOF = True
            return False
        else:
            return True
