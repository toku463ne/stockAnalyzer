import numpy as np
import math

def sma(p, span=20):
    v = np.array(p)
    x = np.convolve(v, np.ones(span), 'valid') / span
    date_start_index = span -1 
    return x.tolist(), date_start_index



def old2_zigzag(ep, dt, h, l, size=5, peak_num=0):
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


def zigzag(ep, dt, h, l, size=5, middle_size=2, peak_num=0):
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
                    
                            



    def old_updateZigZag(newdir, i, p):
        olddir = 0
        lastdir = 0
        if len(peakidxs) > 0:
            j = peakidxs[-1]
            olddir = dirs[-1]
            do_pop = False
            ldirs = len(dirs)
            if newdir*olddir > 0:
                if newdir >= 1:
                    if p[i] > h[j]:
                        do_pop = True
                        lastdir = 1
                else:
                    if p[i] < l[j]:
                        do_pop = True
                        lastdir = -1
                if do_pop:
                    dirs[-1] = lastdir
                    #peakidxs.pop()
            elif ldirs >= 2:
                lastdir = dirs[-1]
                if abs(lastdir) == 2:
                    k = 2
                    while k <= ldirs:
                        if dirs[-k]*lastdir < 0:
                            break
                        if dirs[-k] == lastdir:
                            do_pop = True
                            lastdir = dirs[-1]/2
                            break
                        k += 1
                if do_pop:
                    dirs[-1] = lastdir
                    #peakidxs.pop()
            
        if newdir != olddir or do_pop:
            if (len(dirs) < 0 and newdir*dirs[-1] > 0) and do_pop == False:
                return
            dirs.append(newdir)
            peakidxs.append(i)
    
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




def old3_zigzag(ep, dt, h, l, size=5, middle_size=2, peak_num=0):
    peakidxs = []
    dirs = []

    def _updateZigZag(newdir, i, p):
        olddir = 0
        if len(peakidxs) > 0:
            j = peakidxs[-1]
            olddir = dirs[-1]
            do_pop = False
            if newdir == olddir:
                if newdir >= 1:
                    if p[i] > h[j]:
                        do_pop = True
                else:
                    if p[i] < l[j]:
                        do_pop = True
                if do_pop:
                    dirs.pop()
                    peakidxs.pop()
        if newdir != olddir or do_pop:
            dirs.append(newdir)
            peakidxs.append(i)
    
    mididxs = []
    middirs = []
    for i in range(len(ep)-size*2, 0, -1):
        midi = i + size
        midh = h[midi]
        midl = l[midi]
        
        if i+size*2 <= len(ep) and midh == max(h[i:i+size*2+1]):
            _updateZigZag(1, midi, h)
        if i+size*2 <= len(ep) and midl == min(l[i:i+size*2+1]):
            _updateZigZag(-1, midi, l)
        
    
        midi = i + size

        #import lib
        #from datetime import datetime
        #if lib.epoch2dt(ep[midi]) == datetime(2022,1, 21):
        #    print("here")

        midh = h[midi]
        midl = l[midi]

        if midh == max(h[i:i+size+middle_size+1]):
            mididxs.append(midi)
            middirs.append(1)
        elif midl == min(l[i:i+size+middle_size+1]):
            mididxs.append(midi)
            middirs.append(-1)

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

    mididxs.reverse()
    middirs.reverse()
    midep = []
    middt = []
    midprices = []
    for i in range(len(mididxs)):
        idx = mididxs[i]
        d = middirs[i]
        if d > 0:
            midprices.append(h[idx])
        if d < 0:
            midprices.append(l[idx])
        midep.append(ep[idx])
        middt.append(dt[idx])
    
    return newep, newdt, dirs, prices, dists, midep, middt, middirs, midprices, date_start_index




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