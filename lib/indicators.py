import numpy as np

def sma(p, span=20):
    v = np.array(p)
    x = np.convolve(v, np.ones(span), 'valid') / span
    date_start_index = span -1 
    return x.tolist(), date_start_index


def zigzag(ep, dt, h, l, size=5, peak_num=0):
    peakidxs = []
    newep = []
    newdt = []
    prices = []
    dirs = []

    def _updateZigZag(newdir, i, p):
        olddir = 0
        if len(peakidxs) > 0:
            j = peakidxs[-1]
            olddir = dirs[-1]
            do_pop = False
            if newdir == olddir:
                if newdir == 1:
                    if p[i] > h[j]:
                        do_pop = True
                else:
                    if p[i] < l[j]:
                        do_pop = True
                if do_pop:
                    dirs.pop()
                    prices.pop()
                    newdt.pop()
                    newep.pop()
                    peakidxs.pop()
        if newdir != olddir or do_pop:
            newdt.append(dt[i])
            dirs.append(newdir)
            prices.append(p[i])
            newep.append(ep[i])
            peakidxs.append(i)
    
    for i in range(len(ep)-size*2+1, 0, -1):
        midi = i + size - 1
        midh = h[midi]
        midl = l[midi]
        
        if midh == max(h[i-1:i+size*2-2]):
            _updateZigZag(1, midi, h)
        if midl == min(l[i-1:i+size*2-2]):
            _updateZigZag(-1, midi, l)
        
        if peak_num > 0 and len(peakidxs) >= peak_num:
            break

    date_start_index = size*2-1
    newep.reverse()
    newdt.reverse()
    dirs.reverse()
    prices.reverse()
    return newep, newdt, dirs, prices, date_start_index

def old_zigzag(ep, dt, h, l, size=5):
    peakidxs = []
    newep = []
    newdt = []
    prices = []
    dirs = []

    def _updateZigZag(newdir, i, p):
        olddir = 0
        if len(peakidxs) > 0:
            j = peakidxs[-1]
            olddir = dirs[-1]
            do_pop = False
            if newdir == olddir:
                if newdir == 1:
                    if p[i] > h[j]:
                        do_pop = True
                else:
                    if p[i] < l[j]:
                        do_pop = True
                if do_pop:
                    dirs.pop()
                    prices.pop()
                    newdt.pop()
                    newep.pop()
                    peakidxs.pop()
        if newdir != olddir or do_pop:
            newdt.append(dt[i])
            dirs.append(newdir)
            prices.append(p[i])
            newep.append(ep[i])
            peakidxs.append(i)
    
    for i in range(size*2-1, len(ep)):
        midi = i - size + 1
        midh = h[midi]
        midl = l[midi]
        
        if midh == max(h[i-size*2+2:i+1]):
            _updateZigZag(1, midi, h)
        if midl == min(l[i-size*2+2:i+1]):
            _updateZigZag(-1, midi, l)
        
    date_start_index = size*2-1
    return newep, newdt, dirs, prices, date_start_index