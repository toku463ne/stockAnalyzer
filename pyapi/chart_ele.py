import lib.indicators as libind


def get_sma_chart_values(name, ep, prices, span):
    x, start_i = libind.sma(prices, span)
    values = []
    for i in range(len(x)):
        item = {}
        item["Date"] = ep[i+start_i]*1000
        item[name] = x[i]
        values.append(item)
    return values

def get_zigzag_chart_values(name, ep, dt, h, l, size):
    #newep, newdt, dirs, prices, dists, date_start_index
    xep, _, dirs, x, _, _ = libind.zigzag(ep, dt, h, l, size)
    values1 = []
    values2 = []
    for i in range(len(x)):
        if abs(dirs[i]) == 2:
            item = {}
            item["Date"] = xep[i]*1000
            item[name] = x[i]
            values1.append(item)
        if abs(dirs[i]) == 1:
            item = {}
            item["Date"] = xep[i]*1000
            item[name] = x[i]
            values2.append(item)
    
    return {"main": values1, "middle": values2}
    
    