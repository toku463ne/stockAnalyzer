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
    xep, _, _, x, _ = libind.zigzag(ep, dt, h, l, size)
    values = []
    for i in range(len(x)):
        item = {}
        item["Date"] = xep[i]*1000
        item[name] = x[i]
        values.append(item)
    return values

    