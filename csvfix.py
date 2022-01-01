import csv
import os
from pycoingecko import CoinGeckoAPI
import time
import numpy as np
import dateparser
import pytz
from datetime import datetime
import pandas as pd

def date_to_milliseconds(date_str):
    """Convert UTC date to milliseconds
    If using offset strings add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"
    See dateparse docs for formats http://dateparser.readthedocs.io/en/latest/
    :param date_str: date in readable format, i.e. "January 01, 2018", "11 hours ago UTC", "now UTC"
    :type date_str: str
    """
    # get epoch value in UTC
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
    # parse our date string
    d = dateparser.parse(date_str)
    # if the date is not timezone aware apply UTC timezone
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=pytz.utc)

    # return the difference in time
    return int((d - epoch).total_seconds() * 1000.0)

cg = CoinGeckoAPI()
tokid = {}
aa = cg.get_coins_list(include_platform='false')
for a in os.listdir("C:/Users/gooda/Documents/GitHub/qlib/output"):
    b = a.split('.')[0].split('BUSD')[0].lower()
    for tokn in aa:
        if tokn['symbol'] == b:
            tokid[a] = tokn['id']

    #with open(os.path.join ("C:/Users/gooda/Documents/GitHub/qlib/binancedata_red", a), 'r', newline='') as read_csv:


def lookfor(list, ind):
    for a in list:
        if a[0] == ind:
            return a[1]

for curtok in tokid:
    #curtok = "BAKEBUSD.csv"
    initdate = 0
    cnt = 0
    it = 0
    df = pd.read_csv(os.path.join("C:/Users/gooda/Documents/GitHub/qlib/output", curtok),skiprows=1,names=['date','open','high','low','close','volume','symbol','QAV','numberoftrades','takerbuyBAV','takerbuyQAV','factor'])
    df['factor']=df['factor'].fillna(1)
    lowtime = date_to_milliseconds(df.date.values[0])/1000
    try:
        cval = cg.get_coin_market_chart_range_by_id(id=tokid[curtok], vs_currency="usd", from_timestamp=lowtime, to_timestamp=1640044800)          
    except:                                                                                                                   
        print('error grabbing')
        time.sleep(60)
        cval = cg.get_coin_market_chart_range_by_id(id=tokid[curtok], vs_currency="usd", from_timestamp=lowtime, to_timestamp=1640044800)
    



    
    # df.set_index('date')
    d0 = [datetime.fromtimestamp(item[0]//1000.0).strftime("%Y-%m-%d") for item in cval['market_caps']]
    
    #d1 = [item[1] for item in cval['market_caps']]
    p0 = [item[1] for item in cval['prices']]
    
    df = df[::-1]
    
    mcpser = pd.DataFrame(cval['market_caps'],columns=['date','factor'])
    mcpser['factor']=mcpser['factor'].fillna(1)
    prices = pd.DataFrame(cval['prices'],columns=['date','price'])
    prices['price']=prices['price'].fillna(1)
    mcpser.factor = mcpser.factor.div(prices.price.values)
    
    try:
        mcpser.factor = mcpser.factor.div(mcpser.factor.values[0]).rdiv(1)
        mcpser = mcpser[::-1]
    except:
        mcpser.factor = np.ones([len(prices.price.values)])
        mcpser = mcpser[::-1]
    try:
        initdd = mcpser.date.values[0]
    except:
        initdd = 0
   
    tts = 0
    for en, fac in enumerate(df['date']):
        t = datetime.fromtimestamp(initdd//1000.0).strftime("%Y-%m-%d")
        if fac.split(' ')[0] == t:

            if df.factor.values[en] == 'inf' or 'NaN' or 0:
                df.factor.values[en] = 1
            try:
                if mcpser.factor.values[tts] <= 1:
                    df.factor.values[en] = mcpser.factor.values[tts]
                else:
                    mcpser.factor.values[tts] = 1
                    df.factor.values[en] = mcpser.factor.values[tts]
            except:
                tts = tts - 1
                if mcpser.factor.values[tts] <= 1:
                    df.factor.values[en] = mcpser.factor.values[tts]
                else:
                    mcpser.factor.values[tts] = 1
                    df.factor.values[en] = mcpser.factor.values[tts]
            initdd = mcpser.date.values[tts]
            tts = tts + 1
        else:
            if df.factor.values[en] == 'inf' or 'NaN' or 0:
                df.factor.values[en] = 1
            if len(mcpser.factor.values) == 0:
                df.factor.values[en] = 1
            else:
                try:
                    if mcpser.factor.values[tts] <= 1:
                        df.factor.values[en] = mcpser.factor.values[tts]
                    else: 
                        mcpser.factor.values[tts] = 1
                        df.factor.values[en] = mcpser.factor.values[tts]
                except:
                    df.factor.values[en] =1
        # else:
        #     df.factor.values[en] = 1
             
            # for aaa in :
            #     if fac.split(' ')[0] == datetime.fromtimestamp(aaa[1].date//1000.0).strftime("%Y-%m-%d"):
            #         #bb = datetime.fromtimestamp(aaa//1000.0).strftime("%Y-%m-%d")
            #         df.factor.values[en] = aaa[1].factor

    df.replace([np.inf, -np.inf], 1, inplace=True)
    df['factor']=df['factor'].fillna(1)
    mcpser = mcpser[::-1]
    df = df[::-1]
    print(mcpser)
    print(df)
    df.to_csv(os.path.join("C:/Users/gooda/Documents/GitHub/qlib/testingout", curtok))
    # with open(os.path.join("C:/Users/gooda/Documents/GitHub/qlib/output", curtok)) as rc, open(os.path.join("C:/Users/gooda/Documents/GitHub/qlib/outd", curtok), 'w', newline='') as wc:
    #     for enum, line in enumerate(csv.DictReader(rc)):
            
    #         it=it+1
    #         try:
    #             vdate = line['date'].split(' ')[0]
    #         except:
    #             if enum == 0: 
    #                 vdate = list(line.keys())[0].split(' ')[0]
    #             else:
    #                 vdate = list(line.items())[0]
    #                 vdate = vdate[0].split(' ')[0]
    #         if vdate != initdate:
    #             if enum == 0:
    #                 tic = time.perf_counter()
    #             cnt = cnt+1
    #             try:
    #                 initdate = line['date'].split(' ')[0]
    #             except:
    #                 if enum == 0: 
    #                     initdate = list(line.keys())[0].split(' ')[0]
    #                 else:
    #                     initdate = list(line.items())[0]
    #                     initdate = initdate[0].split(' ')[0]
    #             dlist = initdate.split('-')
    #             initdate1 = str(dlist[2] + "-" + dlist[1] + "-" + dlist[0])
    #             dt = date_to_milliseconds(initdate1)
    #             mcp = lookfor(cval['market_caps'], dt)
    #             curprice = lookfor(cval['prices'], dt)
    #             #mcp = mcpp['market_data']['market_cap']['usd']
    #             #curprice = float(line['close'])
                
    #             if mcp == None:
    #                 mcp = cval['market_caps'][0][1]
    #             if curprice == None:
    #                 if cval['prices'][0][1] == 0:
    #                     break
    #                 curprice = cval['prices'][0][1]
    #             if enum == 0:
    #                 BASEfact = mcp/curprice
                


    #             CURRfact = mcp/curprice
                
    #             if CURRfact == 0:
    #                 CURRfact = 1

    #             tpfact = BASEfact/CURRfact
    #             if tpfact == 0:
    #                 tpfact = 1
                

    #         line['factor'], line['mcap'] = tpfact, mcp
    #         wrt = csv.DictWriter(wc, line.keys())
    #         if enum == 0:
    #             wrh = csv.writer(wc)
    #             wrh.writerow(line.keys())
    #         wrt.writerow(line)
            

