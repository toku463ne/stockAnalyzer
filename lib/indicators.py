import numpy as np
import math

def sma(p, span=20):
    v = np.array(p)
    x = np.convolve(v, np.ones(span), 'valid') / span
    date_start_index = span -1 
    return x.tolist(), date_start_index

def old_zigzag(ep, dt, h, l, size=5, middle_size=2, peak_num=0):
    peakidxs = []
    dirs = []
    for i in range(len(ep)-size*2, 0, -1):
        midi = i + size
        midh = h[midi]
        midl = l[midi]

        is_peak = False
        if midh == max(h[i:i+size*2]):
            peakidxs.append(midi)
            dirs.append(2)
            is_peak = True
        if midl == min(l[i:i+size*2]):
            peakidxs.append(midi)
            dirs.append(-2)
            is_peak = True
        if is_peak == False:
            if midh == max(h[i:i+size+middle_size+1]):
                peakidxs.append(midi)
                dirs.append(1)
            if midl == min(l[i:i+size+middle_size+1]):
                peakidxs.append(midi)
                dirs.append(-1)
        
        if peak_num > 0 and len(peakidxs) >= peak_num:
            break

    date_start_index = size*2-1
    dirs.reverse()
    peakidxs.reverse()

    newep = []
    newdt = []
    prices = []
    dists = [0]
    oldidx = 0
    for i in range(len(peakidxs)):
        idx = peakidxs[i]
        d = dirs[i]
        if d > 0:
            prices.append(h[idx])
        if d < 0:
            prices.append(l[idx])
        newep.append(ep[idx])
        newdt.append(dt[idx])
        if i > 0:
            dists.append(idx - oldidx)
        oldidx = idx
    
    return newep, newdt, dirs, prices, dists, date_start_index

def old2_zigzag(ep, dt, h, l, size=5, middle_size=2, peak_num=0):
    peakidxs = []
    dirs = []

    def _updateZigZag(newdir, i, p):
        if abs(newdir) == 1 or len(dirs) == 0 or newdir*dirs[-1] <= -4:
            peakidxs.append(i)
            dirs.append(newdir)
            return
        if len(dirs) > 0:
            for j in range(1, len(dirs)+1):
                if newdir*dirs[-j] <= -4:
                    break

                jidx = peakidxs[-j]
                if abs(dirs[-j]) == 2:
                    if newdir > 0:
                        if p[i] > p[jidx]:
                            dirs[-j] = 1
                        else:
                            newdir = 1
                            break
                    else:
                        if p[i] < p[jidx]:
                            dirs[-j] = -1
                        else:
                            newdir = -1
                            break
            peakidxs.append(i)
            dirs.append(newdir)
    
    for i in range(len(ep)-size*2, 0, -1):
        midi = i + size
        midh = h[midi]
        midl = l[midi]
        
        #import lib
        #from datetime import datetime
        #if lib.epoch2dt(ep[midi]) == datetime(2022,1, 21):
        #    print("here")

        if midh == max(h[i:i+size*2]):
            _updateZigZag(2, midi, h)
        elif midl == min(l[i:i+size*2]):
            _updateZigZag(-2, midi, l)
        elif midh == max(h[i:i+size+middle_size+1]):
            _updateZigZag(1, midi, h)
        elif midl == min(l[i:i+size+middle_size+1]):
            _updateZigZag(-1, midi, l)
        
        if peak_num > 0 and len(peakidxs) >= peak_num:
            break

    date_start_index = size*2-1
    dirs.reverse()
    peakidxs.reverse()

    newep = []
    newdt = []
    prices = []
    dists = [0]
    oldidx = 0
    for i in range(len(peakidxs)):
        idx = peakidxs[i]
        d = dirs[i]
        if d > 0:
            prices.append(h[idx])
        if d < 0:
            prices.append(l[idx])
        newep.append(ep[idx])
        newdt.append(dt[idx])
        if i > 0:
            dists.append(idx - oldidx)
        oldidx = idx
    
    return newep, newdt, dirs, prices, dists, date_start_index


def zigzag(ep, dt, h, l, v, size=5, middle_size=2, peak_num=0):
    peakidxs = []
    dirs = []

    def _updateZigZag(newdir, i, p):
        if abs(newdir) == 1 or len(dirs) == 0 or newdir*dirs[-1] <= -4:
            peakidxs.append(i)
            dirs.append(newdir)
            return
        if len(dirs) > 0:
            for j in range(1, len(dirs)+1):
                if newdir*dirs[-j] <= -4:
                    break

                jidx = peakidxs[-j]
                if abs(dirs[-j]) == 2:
                    if newdir > 0:
                        if p[i] > p[jidx]:
                            dirs[-j] = 1
                        else:
                            newdir = 1
                            break
                    else:
                        if p[i] < p[jidx]:
                            dirs[-j] = -1
                        else:
                            newdir = -1
                            break
            peakidxs.append(i)
            dirs.append(newdir)
    
    for i in range(len(ep)-size*2, 0, -1):
        midi = i + size
        midh = h[midi]
        midl = l[midi]
        
        #import lib
        #from datetime import datetime
        #if lib.epoch2dt(ep[midi]) == datetime(2022,1, 21):
        #    print("here")

        if midh == max(h[i:i+size*2]):
            _updateZigZag(2, midi, h)
        elif midl == min(l[i:i+size*2]):
            _updateZigZag(-2, midi, l)
        elif midh == max(h[i:i+size+middle_size+1]):
            _updateZigZag(1, midi, h)
        elif midl == min(l[i:i+size+middle_size+1]):
            _updateZigZag(-1, midi, l)
        
        if peak_num > 0 and len(peakidxs) >= peak_num:
            break

    date_start_index = size*2-1
    dirs.reverse()
    peakidxs.reverse()

    newep = []
    newdt = []
    newv = []
    prices = []
    dists = [0]
    oldidx = 0
    for i in range(len(peakidxs)):
        idx = peakidxs[i]
        d = dirs[i]
        if d > 0:
            prices.append(h[idx])
        if d < 0:
            prices.append(l[idx])
        vl = 0
        if i > 0:
            cnt = 0
            cnts = 0
            s = 0
            for j in range(peakidxs[i-1]+1, peakidxs[i]+1):
                s += (cnt+1)*v[j]
                cnts += cnt+1
                cnt += 1
            vl = s/cnts
        newep.append(ep[idx])
        newdt.append(dt[idx])
        newv.append(vl)
        if i > 0:
            dists.append(idx - oldidx)
        oldidx = idx
    
    return newep, newdt, dirs, prices, newv, dists, date_start_index


