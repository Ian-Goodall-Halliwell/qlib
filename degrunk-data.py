from code import compile_command
import re
from sys import maxsize
from binance.client import Client
import time
import csv
import os
import fire
import random
import queue
client1 = Client('igEARWI7LNtjhzHa3zrNAMtLlLtUjnNb3VFHSHCf5Nlnga4h3vAzthAQKe8wLYlC', 'BM8EVK6TI5kHKQ7sORXpkwHet8mtq8alhOV5JJQ25kAIunKL7YkGgfc80inJad0I')
client2 = Client('FCbtPk3mQj2IqpFbvR5rgPdXgZL8O3s4634zP5thOb0ob6MuiG7sxsvdzVy3MSe2','aljR5fx3pHWSEc6JRkN0YMNlNk28rdM5CBE1XCzUvi8MQy4qoG5q7T5QAD3V9E1w')
client3 = Client('yNt4nLpNc4sg4l7ZwFf3uqBbRq2YidMzIrrmAqWNjQMGCcPvTt66CXMl4S7LGyqO','EWx8Fh5VjQrE6PGA9ywkIiAuOs0VX9Hk22dsBgLFdV4EqkE765ov5GxFDCpdbbr0')
client4 = Client('zS74mTu25foQTQP2ttuH6gRN2cfbSnBzuIDMlktZRVvCQvHpq4G3CvQeHEszlSjH','MFxQEWT28n7UQZ3JK6XsXcsWsRp0l8dYPvrZbQOxN9jK5ez11gqnXDt4aCaRJUFO')
client5 = Client('xVHPQDvR2mvDITO9pRi2yFxhxmv1AyqS8cCxJepbH74Kt6XeB9Zn5lTmgbauSg1d','sGx5MBrbi2iMpZKTvwurEuHA1OYPqSwT9DeJjC8ppkmWSokTDjZsbRjmq58nBG1c')
client6 = Client('J9HNtB1mXWiqwaOLyxOQTB6yiy6Vg7ZfLOXdTdYPofc2hI8XDBcuc7yeIv02EtUx','P1aQFvKuFyGOFgnufWKr61o0lPWuQjt1ZwzDDzZ1RsxMuUFDUiqx5uqI4JlT6sPJ')
client7 = Client('ZQCSfbLRqUQifBmltgLf30Lm9gHSiRovZVyAhvsxi7nKA3TEC9ehsnsl1sdkqSct','iIZOWSTGfTsdA1krWEz2sGU0pRjyGVzFUinMkq6eGwgIf45dorp6xCuxIHhXFQdt')
client8 = Client('DocIMuZHP2x0TxprspUUX0eJSDwXsDkhKLvSa0TF6bKH69otCpSLXYqnKPLQsdzv','j8KqFMMET2QzobtX82pe7VhjuA2eWz9ucc088RHgzKLD16QpHFUAVm4QZ4naIDWr')
from pycoingecko import CoinGeckoAPI
import json
from binance.client import Client
import binance.enums
from datetime import date
import dateparser
import pytz
from datetime import datetime
import threading
from threading import Lock

s_print_lock = Lock()

def s_print(*a, **b):
    """Thread safe print function"""
    with s_print_lock:
        print(*a, **b)
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
def date_to_milliseconds1(date_str):
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

def make_dirs():
    dirs = os.path.dirname(os.path.realpath(__file__))
    os.mkdir(os.path.join(dirs, 'data-download'))
    os.mkdir(os.path.join(dirs, 'data-download/day-unprocessed'))
    os.mkdir(os.path.join(dirs, 'data-download/1min-unprocessed'))
    os.mkdir(os.path.join(dirs, 'data-download/day-futures'))
    os.mkdir(os.path.join(dirs, 'data-download/1min-futures'))
    os.mkdir(os.path.join(dirs, 'data-download/day-processed'))
    os.mkdir(os.path.join(dirs, 'data-download/1min-processed'))
    


def get_exchange_info():
    dd =  client8.get_exchange_info()
    return dd
def newest(path):
    files = os.listdir(path)
    paths = [os.path.join(path, basename) for basename in files]
    #try:
    max(paths, key=os.path.getctime)
    #except:
        #print('skippin')
    return 

def checkprog(pth):
    outlist = []
    qqq = os.listdir(pth)
    qqq = os.listdir(pth)
    for qqi in qqq:
        qqi = qqi.split('.')[0]
        outlist.append(qqi)
    
    return outlist

def getstate(outlist, exchangeinfo, tokennames = ['USDT', 'BUSD']):
    currlist = []
    for ab in exchangeinfo['symbols']:
    
        if ab['quoteAsset'] == tokennames[0]:
            if not ab['symbol'] in outlist:
                if not "BEAR" in ab['symbol']:
                    if not "BULL" in ab['symbol']:
                        currlist.append(ab['symbol'])
        if ab['quoteAsset'] == tokennames[1]:
            if not ab['symbol'] in outlist:
                if not "BEAR" in ab['symbol']:
                    if not "BULL" in ab['symbol']:
                        currlist.append(ab['symbol'])
    print("number of tokens:", len(currlist))
    return currlist

def getcgdata(start, end, token, aa,cg):
    ccdcdc = date_to_milliseconds(start)//1000 + 86400
    ccdcdd = date_to_milliseconds(end)//1000 + 86400 + 86400
    ccdccc = date_to_milliseconds(end)//1000
    tokid = {}
    tokidbsd = {}
    b = token.split('.')[0].split('USDT')[0].lower()
    bt = token.split('.')[0].split('BUSD')[0].lower()
    for tokn in aa:
        if tokn['symbol'] == b:
            tokid[token] = tokn['id']
    for tokn in aa:
        if tokn['symbol'] == bt:
            tokidbsd[token] = tokn['id']
    try:
        try:
            cval = cg.get_coin_market_chart_range_by_id(id=tokid[token], vs_currency="usd", from_timestamp=ccdcdc, to_timestamp=ccdcdd)
        except:
            cval = cg.get_coin_market_chart_range_by_id(id=tokidbsd[token], vs_currency="usd", from_timestamp=ccdcdc, to_timestamp=ccdcdd)
    except:
        try:
            cval = cg.get_coin_market_chart_range_by_id(id=token.split('USDT')[0].lower(), vs_currency="usd", from_timestamp=ccdcdc, to_timestamp=ccdcdd)
        except:
            try:
                cval = cg.get_coin_market_chart_range_by_id(id=token.split('BUSD')[0].lower(), vs_currency="usd", from_timestamp=ccdcdc, to_timestamp=ccdcdd)
            except Exception as e: 
                try:
                    if e.response.status_code == 429:
                        #s_print('err 429', token, 'waiting for 60s')
                        time.sleep(60)
                        try:
                            cval = cg.get_coin_market_chart_range_by_id(id=token.split('USDT')[0].lower(), vs_currency="usd", from_timestamp=ccdcdc, to_timestamp=ccdcdd)
                        except:
                            cval = cg.get_coin_market_chart_range_by_id(id=token.split('BUSD')[0].lower(), vs_currency="usd", from_timestamp=ccdcdc, to_timestamp=ccdcdd)
                except:
                    #s_print('skipping', token)
                    return
    #s_print('grabbed', token)
    return cval


def download(start, end, interval, currlist, pth, type, withcgdata=True, withaggtrades=False, aggtradelimit=0):    
    from binance.enums import HistoricalKlinesType
    if withcgdata == True:
        cg = CoinGeckoAPI()
        aa = cg.get_coins_list()
    for token in currlist:
        if withcgdata == True:
            ccdcdc = date_to_milliseconds(start)//1000 
            ccdcdd = date_to_milliseconds(end)//1000 
            ccdccc = date_to_milliseconds(end)//1000
            tokid = {}
            b = token.split('.')[0].split('USDT')[0].lower()
            for tokn in aa:
                if tokn['symbol'] == b:
                    tokid[token] = tokn['id']
            try:
                cval = cg.get_coin_market_chart_range_by_id(id=tokid[token], vs_currency="usd", from_timestamp=ccdcdc, to_timestamp=ccdcdd)
            except:
                try:
                    cval = cg.get_coin_market_chart_range_by_id(id=token.split('USDT')[0].lower(), vs_currency="usd", from_timestamp=ccdcdc, to_timestamp=ccdcdd)
                except Exception as e: 
                    
                    try:
                        if e.response.status_code == 429:
                            time.sleep(60)
                            cval = cg.get_coin_market_chart_range_by_id(id=token.split('USDT')[0].lower(), vs_currency="usd", from_timestamp=ccdcdc, to_timestamp=ccdcdd)
                    except:
                        continue
                    

        klines = client8.get_historical_klines(token, interval, start, end,  klines_type= HistoricalKlinesType.SPOT)
                    

        if not klines == []:
            klines.pop(-1)
        else:
            continue
        if len(cval['market_caps']) != 0:
            d1 = [item[1] for item in cval['market_caps']]
        else:
            d1 = []
            d1.extend([0] * len(klines))
        if len(cval['total_volumes']) != 0:
            d2 = [item[1] for item in cval['total_volumes']]
        else: 
            d2 = []
            d2.extend([0] * len(klines))
        if len(cval['prices']) != 0:
            d3 = [item[1] for item in cval['prices']]
        else:
            d3 = []
            d3.extend([0] * len(klines))
        if withcgdata == True:
            with open(os.path.join(pth,'{}.csv'.format(token)), 'w', newline='') as f:
                writerc = csv.writer(f)
                if withaggtrades == False:
                    dic = ['date', 'open', 'high','low', 'close','volume','symbol','QAV','numberoftrades','takerbuyBAV','takerbuyQAV','market_cap', 'total_volume','price_from_coingecko', 'factor']
                    writerc.writerow(dic)
                gct = 0
                it = 0
                prevdate = 0
                testvar = len(klines)//60
                testvar2 = (len(klines)+ 60)//60
                if type == '1min':
                    if len(d1) > (len(klines)+ 60)//60:
                        cnct = len(d1) - ((len(klines)//60) + 1)
                        mnx = 0
                    elif len(d1) == (len(klines) + 60)//60:
                        cnct = 0 
                        mnx = 0
                    
                    elif len(d1) == len(klines)//60:
                        cnct = 0 
                        mnx = 0
                    else:
                        cnct = 0
                        mnx = len(klines)//60 - len(d1) 
                if type == 'day':
                    if len(d1) > (len(klines) + 1):
                        cnct = len(d1) - ((len(klines)) + 1)
                        mnx = 0
                    elif len(d1) == (len(klines) + 1):
                        cnct = 0 
                        mnx = 0
                    
                    elif len(d1) == len(klines):
                        cnct = 0 
                        mnx = 1
                    else:
                        cnct = 0
                        mnx = (len(klines) + 1) - len(d1) 
                templist = []
                for a in klines: 
                    if withaggtrades == True:
                        olis = []
                        if type == 'day':
                            agg_trades = client8.aggregate_trade_iter(symbol=token, start_str=a[0], endingm=a[6], day=True)
                        if type == '1min':
                            agg_trades = client8.aggregate_trade_iter(symbol=token, start_str=a[0], endingm=a[6], day=False)
                        dic = ['date', 'open', 'high','low', 'close','volume','symbol','QAV','numberoftrades','takerbuyBAV','takerbuyQAV','market_cap', 'total_volume','price_from_coingecko', 'factor']
                        
                        for enumr, qqq in enumerate(range(1000)):
                            dic.append('p_{}'.format(enumr))
                            dic.append('q_{}'.format(enumr))
                            dic.append('m_{}'.format(enumr))
                            dic.append('M_{}'.format(enumr))
                        writerc.writerow(dic)
                        for bb in agg_trades:
                            for bbb in bb:
                                if bbb == []:
                                    continue
                                for bbbbb in bbb:
                                    olis.append(bbbbb)
                        fnlis = sorted(olis, key = lambda i: i['q'], reverse=True)[0:1000]
                    if type == 'day':
                        if datetime.fromtimestamp(a[0]/1000.0,tz=pytz.utc).strftime("%d") != prevdate:
                            cnct = cnct + 1
                            prevdate = datetime.fromtimestamp(a[0]/1000.0,tz=pytz.utc).strftime("%d")
                            if mnx != None:
                                if mnx != 0:
                                    mnx = mnx -1
                                    cnct = cnct -1
                        dd = datetime.fromtimestamp(a[0]/1000.0,tz=pytz.utc).strftime("%Y-%m-%d")
                        b = [dd, a[1], a[2], a[3], a[4], a[5],token,a[7],a[8],a[9],a[10],d1[cnct],d2[cnct],d3[cnct],1]
                        if withaggtrades == True:
                            for enumr, qqq in enumerate(range(1000)):
                                try:
                                    b.append(fnlis[enumr]['p'])
                                    b.append(fnlis[enumr]['q'])
                                    b.append(fnlis[enumr]['m'])
                                    b.append(fnlis[enumr]['M'])
                                except:
                                    b.append(0)
                                    b.append(0)
                                    b.append(0)
                                    b.append(0)
                        templist.append(b)
                        gct = gct + 1
                        if gct == 10000:
                            it = it + 1
                            gct= 0
                            #print('{} Iteration #: '.format(token), it)
                            writerc.writerows(templist)
                            templist = []
                        writerc.writerows(templist)
                        templist = []
                    if type == '1min':
                        dte = int(datetime.fromtimestamp(a[0]/1000.0,tz=pytz.utc).strftime("%H"))
                        if  dte != prevdate:
                            cnct = cnct + 1
                            prevdate = int(datetime.fromtimestamp(a[0]/1000.0,tz=pytz.utc).strftime("%H"))
                            if mnx != None:
                                if mnx != 0:
                                    mnx = mnx -1
                                    cnct = cnct -1
                        dd = datetime.fromtimestamp(a[0]/1000.0,tz=pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
                        b = [dd, a[1], a[2], a[3], a[4], a[5],token,a[7],a[8],a[9],a[10],d1[cnct],d2[cnct],d3[cnct],1]
                        if withaggtrades == True:
                            for enumr, qqq in enumerate(range(1000)):
                                try:
                                    b.append(fnlis[enumr]['p'])
                                    b.append(fnlis[enumr]['q'])
                                    b.append(fnlis[enumr]['m'])
                                    b.append(fnlis[enumr]['M'])
                                except:
                                    b.append(0)
                                    b.append(0)
                                    b.append(0)
                                    b.append(0)
                    
                    
                        templist.append(b)
                        gct = gct + 1
                        if gct == 10000:
                            it = it + 1
                            gct= 0
                            #print('{} Iteration #: '.format(token), it)
                            writerc.writerows(templist)
                            templist = []
                        writerc.writerows(templist)
                        templist = []
        else:
            with open(os.path.join(pth,'{}.csv'.format(token)), 'w', newline='') as f:
                writerc = csv.writer(f)
                dic = ['date', 'open', 'high','low', 'close','volume','symbol','QAV','numberoftrades','takerbuyBAV','takerbuyQAV','factor']
                writerc.writerow(dic)
                gct = 0
                it = 0
                prevdate = 0
                for a in klines:
                    if datetime.fromtimestamp(a[0]/1000.0,tz=pytz.utc).strftime("%d") != prevdate:
                        cnct = cnct + 1
                        prevdate = datetime.fromtimestamp(a[0]/1000.0,tz=pytz.utc).strftime("%d")
                    
                    if type == 'day':
                        dd = datetime.fromtimestamp(a[0]/1000.0,tz=pytz.utc).strftime("%Y-%m-%d")
                    if type == '1min':
                        dd = datetime.fromtimestamp(a[0]/1000.0,tz=pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
                    b = [dd, a[1], a[2], a[3], a[4], a[5],token,a[7],a[8],a[9],a[10],1]
                    writerc.writerow(b)
                    gct = gct + 1
                    if gct == 1000:
                        it = it + 1
                        gct= 0
                        #print('{} Iteration #: '.format(token), it)
                writerc.writerows(templist)

def download1(start, end, interval, q, pth, type, client, pq, aa, withcgdata=True, withaggtrades=False, aggtradelimit=0):    
    from binance.enums import HistoricalKlinesType
    # if withcgdata == True:
        
    if type == '1min':
        token = q.get()
    if type == 'day':
        token = q
    
    if token == None:
        return
    if type == '1min':
        order = pq.get()
    if type == 'day':
        order = pq
    #print(order)
    if withcgdata == True:
        cval = getcgdata(start, end, token, aa,cg)
        if cval == None:
            if type == '1min':
                pq.put(order)
                return
            if type == 'day':
                return
    try:        
        klines = client.get_historical_klines(token, interval, start, end,  klines_type= HistoricalKlinesType.SPOT,barorder=order)
    except:
        try:
            time.sleep(random.randint(15,180))
            klines = client.get_historical_klines(token, interval, start, end,  klines_type= HistoricalKlinesType.SPOT,barorder=order)
        except:
            pq.put(order)
            return
    
    if type == '1min':        
        if not klines == []:
            klines.pop(-1)
        else:
            pq.put(order)
            return
    if len(cval['market_caps']) != 0:
        d1 = [item[1] for item in cval['market_caps']]
    else:
        d1 = []
        d1.extend([0] * (len(klines)+1))
    if len(cval['total_volumes']) != 0:
        d2 = [item[1] for item in cval['total_volumes']]
    else: 
        d2 = []
        d2.extend([0] * (len(klines)+1))
    if len(cval['prices']) != 0:
        d3 = [item[1] for item in cval['prices']]
    else:
        d3 = []
        d3.extend([0] * (len(klines)+1))
    if withcgdata == True:
        with open(os.path.join(pth,'{}.csv'.format(token)), 'w', newline='') as f:
            writerc = csv.writer(f)
            if withaggtrades == False:
                dic = ['date', 'open', 'high','low', 'close','volume','symbol','QAV','numberoftrades','takerbuyBAV','takerbuyQAV','market_cap', 'total_volume','price_from_coingecko', 'factor']
                writerc.writerow(dic)
            gct = 0
            it = 0
            if klines == []:
                return
            prevdate = datetime.fromtimestamp(klines[0][0]/1000.0,tz=pytz.utc).strftime("%d")
            testvar = len(d1) - 2
            testvar2 = (len(klines))//60
            cctf = testvar2/testvar
            if type == '1min':
                if len(d1) > (len(klines)//60)//24:
                    
                    cnct = (len(d1)) - (len(klines)//60)//24
                    cnct1 = (len(d1)) - ((len(klines)//60)//24 + 1)
                    #cnct1 = (len(d1) -2) - (len(klines)//60)//24
                    #cnct2 = (len(d1) -3) - (len(klines)//60)//24
                    mnx = 0
                elif (len(klines)//60)//24 >= len(d1):
                    cnct = 0
                    
                    mnx = (((len(klines))//60)//24 + 2) - len(d1)
            if type == 'day':
                if len(d1) > len(klines):
                    
                    cnct = (len(d1)) - len(klines)
                    #cnct1 = (len(d1)) - len(klines)
                    #cnct2 = (len(d1) -3) - len(klines)
                    mnx = 0
                elif len(klines) >= len(d1):
                    cnct = 0
                    
                    mnx =  (len(klines) + 1) - len(d1)
            templist = []
            for enm, a in enumerate(klines): 
                if withaggtrades == True:
                    olis = []
                    if type == 'day':
                        agg_trades = client.aggregate_trade_iter(symbol=token, start_str=a[0], endingm=a[6], day=True)
                    if type == '1min':
                        agg_trades = client.aggregate_trade_iter(symbol=token, start_str=a[0], endingm=a[6], day=False)
                    dic = ['date', 'open', 'high','low', 'close','volume','symbol','QAV','numberoftrades','takerbuyBAV','takerbuyQAV','market_cap', 'total_volume','price_from_coingecko', 'factor']
                    
                    for enumr, qqq in enumerate(range(1000)):
                        dic.append('p_{}'.format(enumr))
                        dic.append('q_{}'.format(enumr))
                        dic.append('m_{}'.format(enumr))
                        dic.append('M_{}'.format(enumr))
                    writerc.writerow(dic)
                    for bb in agg_trades:
                        for bbb in bb:
                            if bbb == []:
                                continue
                            for bbbbb in bbb:
                                olis.append(bbbbb)
                    fnlis = sorted(olis, key = lambda i: i['q'], reverse=True)[0:1000]
                if type == 'day':
                    if datetime.fromtimestamp(a[0]/1000.0,tz=pytz.utc).strftime("%d") != prevdate:
                        cnct = cnct + 1
                        
                        prevdate = datetime.fromtimestamp(a[0]/1000.0,tz=pytz.utc).strftime("%d")
                        if mnx != None:
                            if mnx != 0:
                                mnx = mnx -1
                                cnct = cnct -1
                                
                    dd = datetime.fromtimestamp(a[0]/1000.0,tz=pytz.utc).strftime("%Y-%m-%d")
                    # try:
                    cnct_temp = cnct
                    for t in range(cnct+1):
                        try:
                            b = [dd, a[1], a[2], a[3], a[4], a[5],token,a[7],a[8],a[9],a[10],d1[cnct_temp],d2[cnct_temp],d3[cnct_temp],1]
                            break
                        except:
                            cnct_temp = cnct_temp -1
                            if cnct_temp == 0:
                                return
                            continue
                        
                        
                    # except:
                    #     try:
                    #         b = [dd, a[1], a[2], a[3], a[4], a[5],token,a[7],a[8],a[9],a[10],d1[cnct],d2[cnct],d3[cnct],1]
                    #         print('for', token, 'unsing cnct1')
                    #     except:
                    #         b = [dd, a[1], a[2], a[3], a[4], a[5],token,a[7],a[8],a[9],a[10],d1[cnct2],d2[cnct2],d3[cnct2],1]
                    #         print('for', token, 'unsing cnct2')
                    if withaggtrades == True:
                        for enumr, qqq in enumerate(range(1000)):
                            try:
                                b.append(fnlis[enumr]['p'])
                                b.append(fnlis[enumr]['q'])
                                b.append(fnlis[enumr]['m'])
                                b.append(fnlis[enumr]['M'])
                            except:
                                b.append(0)
                                b.append(0)
                                b.append(0)
                                b.append(0)
                    templist.append(b)
                    gct = gct + 1
                    if gct == 10000:
                        it = it + 1
                        gct= 0
                        #print('{} Iteration #: '.format(token), it)
                        writerc.writerows(templist)
                        templist = []
                if type == '1min':
                    dte = int(datetime.fromtimestamp(a[0]/1000.0,tz=pytz.utc).strftime("%d"))
                    if  dte != prevdate:
                        cnct = cnct + 1
                        
                        prevdate = int(datetime.fromtimestamp(a[0]/1000.0,tz=pytz.utc).strftime("%d"))
                        if mnx != None:
                            if mnx != 0:
                                mnx = mnx -1
                                cnct = cnct -1
                                
                    dd = datetime.fromtimestamp(a[0]/1000.0,tz=pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
                    #b = [dd, a[1], a[2], a[3], a[4], a[5],token,a[7],a[8],a[9],a[10],d1[cnct],d2[cnct],d3[cnct],1]
                    # try:
                    #     b = [dd, a[1], a[2], a[3], a[4], a[5],token,a[7],a[8],a[9],a[10],d1[cnct+3],d2[cnct+3],d3[cnct+3],1]
                    # except:
                    #     try:
                    #         b = [dd, a[1], a[2], a[3], a[4], a[5],token,a[7],a[8],a[9],a[10],d1[cnct+2],d2[cnct+2],d3[cnct+2],1]
                    #     except:    
                    #         try:
                    #             b = [dd, a[1], a[2], a[3], a[4], a[5],token,a[7],a[8],a[9],a[10],d1[cnct+1],d2[cnct+1],d3[cnct+1],1]
                    #         except:
                    cnct_temp = cnct
                    for t in range(cnct):
                        
                        try:
                            b = [dd, a[1], a[2], a[3], a[4], a[5],token,a[7],a[8],a[9],a[10],d1[cnct_temp],d2[cnct_temp],d3[cnct_temp],1]
                            break
                        except:
                            cnct_temp = cnct_temp -1
                            if cnct_temp == 0:
                                return
                            continue
                    # try:
                    #     b = [dd, a[1], a[2], a[3], a[4], a[5],token,a[7],a[8],a[9],a[10],d1[cnct],d2[cnct],d3[cnct],1]
                    # except:
                    #     try:
                    #         b = [dd, a[1], a[2], a[3], a[4], a[5],token,a[7],a[8],a[9],a[10],d1[cnct1],d2[cnct1],d3[cnct1],1]
                    #     except:
                    #         try:
                    #             b = [dd, a[1], a[2], a[3], a[4], a[5],token,a[7],a[8],a[9],a[10],d1[cnct1 - 1],d2[cnct1 - 1],d3[cnct1 - 1],1]
                    #         except:
                    #             try:
                    #                 b = [dd, a[1], a[2], a[3], a[4], a[5],token,a[7],a[8],a[9],a[10],d1[cnct1 - 2],d2[cnct1 - 2],d3[cnct1 - 2],1]
                    #             except:
                    #                 try:
                    #                     b = [dd, a[1], a[2], a[3], a[4], a[5],token,a[7],a[8],a[9],a[10],d1[cnct1 - 3],d2[cnct1 - 3],d3[cnct1 - 3],1]
                    #                 except:
                    #                     try:
                    #                         b = [dd, a[1], a[2], a[3], a[4], a[5],token,a[7],a[8],a[9],a[10],d1[cnct1 - 4],d2[cnct1 - 4],d3[cnct1 - 4],1]
                    #                     except:
                    #                         pq.put(order)
                    #                         return
                                
                    if withaggtrades == True:
                        for enumr, qqq in enumerate(range(1000)):
                            try:
                                b.append(fnlis[enumr]['p'])
                                b.append(fnlis[enumr]['q'])
                                b.append(fnlis[enumr]['m'])
                                b.append(fnlis[enumr]['M'])
                            except:
                                b.append(0)
                                b.append(0)
                                b.append(0)
                                b.append(0)
                
                
                    try:
                        templist.append(b)
                    except:
                        return
                    gct = gct + 1
                    if gct == 10000:
                        it = it + 1
                        gct= 0
                        #print('{} Iteration #: '.format(token), it)
                        writerc.writerows(templist)
                        templist = []
            writerc.writerows(templist)
            templist = []
    else:
        with open(os.path.join(pth,'{}.csv'.format(token)), 'w', newline='') as f:
            writerc = csv.writer(f)
            dic = ['date', 'open', 'high','low', 'close','volume','symbol','QAV','numberoftrades','takerbuyBAV','takerbuyQAV','factor']
            writerc.writerow(dic)
            gct = 0
            it = 0
            prevdate = 0
            templist = []
            for a in klines:
                if datetime.fromtimestamp(a[0]/1000.0,tz=pytz.utc).strftime("%d") != prevdate:
                    cnct = cnct + 1
                    prevdate = datetime.fromtimestamp(a[0]/1000.0,tz=pytz.utc).strftime("%d")
                
                if type == 'day':
                    dd = datetime.fromtimestamp(a[0]/1000.0,tz=pytz.utc).strftime("%Y-%m-%d")
                if type == '1min':
                    dd = datetime.fromtimestamp(a[0]/1000.0,tz=pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
                b = [dd, a[1], a[2], a[3], a[4], a[5],token,a[7],a[8],a[9],a[10],1]
                writerc.writerow(b)
                gct = gct + 1
                if gct == 1000:
                    it = it + 1
                    gct= 0
                    #print('{} Iteration #: '.format(token), it)
            writerc.writerows(templist)

   
            


def takehalf(v, list):
    length = len(list)
    middle_index = length//2

    if v == 1:
        first_half = list[:middle_index]
        return first_half
    if v == 2:
        second_half = list[middle_index:]
        return second_half
def splitlist(x, list):
    x = len(list)//x + 1
    final_list= lambda test_list, x: [test_list[i:i+x] for i in range(0, len(test_list), x)]
    output=final_list(list, x)
    return output

def startdownload_day(withcgdata=True, withaggtrades=False, aggtradelimit=0, run=True, ver=None):
    if run == True:
        global cg
        cg = CoinGeckoAPI()
        try:
            aa = cg.get_coins_list()
        except:
            time.sleep(120)
            aa = cg.get_coins_list()
            
        dirs = os.path.dirname(os.path.realpath(__file__))
        if withcgdata == True:
            pth = os.path.join(dirs, 'data-download/day-unprocessed')
        else:
            pth = os.path.join(dirs, 'data-download/day-processed')
        exchg = get_exchange_info()
        currlist = getstate(checkprog(pth),exchg)
        q = queue.SimpleQueue()
        for item in currlist:
            q.put(item)
        pq = queue.SimpleQueue()
        for itm in range(4):
            pq.put(itm)
        start = "21 Dec, 2010"
        end = "4 Jan, 2022"
        interval = Client.KLINE_INTERVAL_1DAY
        if not ver == None:
            currlist = takehalf(ver, currlist)
            download(start,end,interval,currlist,pth,'day',withaggtrades=False,aggtradelimit=0)
        if ver == None:
            # l1,l2,l3,l4 = splitlist(4,currlist)
            
            for a in currlist:
                download1(start,end,interval,a,pth,'day',client1, 0, aa, withaggtrades=False,aggtradelimit=0)
                # t1 = threading.Thread(target=download1, args=(start,end,interval,a,pth,'day',client1, 0))
                # t2 = threading.Thread(target=download1, args=(start,end,interval,q,pth,'day',client2, pq))
                # t3 = threading.Thread(target=download1, args=(start,end,interval,q,pth,'day',client3, pq))
                # t4 = threading.Thread(target=download1, args=(start,end,interval,q,pth,'day',client4, pq))
                
                # t1.start()
                # time.sleep(10)
                # t2.start()
                # time.sleep(10)
                # t3.start()
                # time.sleep(10)
                # t4.start()
            
            
            
            
                # t1.join()
                # t2.join()
                # t3.join()
                # t4.join()
            
            

def startdownload_1min(withcgdata=True, withaggtrades=False, aggtradelimit=0, run=False, ver=None):
    global cg
    if run == True:
        cg = CoinGeckoAPI()
        try:
            aa = cg.get_coins_list()
        except:
            time.sleep(120)
            aa = cg.get_coins_list()
        
        dirs = os.path.dirname(os.path.realpath(__file__))
        if withcgdata == True:
            pth = os.path.join(dirs, 'data-download/1min-unprocessed')
        else:
            pth = os.path.join(dirs, 'data-download/1min-processed')
        exchg = get_exchange_info()
        currlist = getstate(checkprog(pth),exchg)
        start = "21 Dec, 2010"
        end = "5 Jan, 2022"
        q = queue.SimpleQueue()
        for item in currlist:
            q.put(item)
        pq = queue.SimpleQueue()
        for itm in range(8):
            pq.put(itm)
        interval = Client.KLINE_INTERVAL_1MINUTE
        if not ver == None:
            currlist = takehalf(ver, currlist)
            download(start,end,interval,currlist,pth,'1min',withaggtrades=False,aggtradelimit=0)
        if ver == None:
            #l1,l2,l3,l4,l5,l6,l7,l8 = splitlist(8,currlist)
            
            for a in (range((len(currlist)//8)+ 1)):
                t1 = threading.Thread(target=download1, args=(start,end,interval,q,pth,'1min',client1, pq,aa))
                t2 = threading.Thread(target=download1, args=(start,end,interval,q,pth,'1min',client2, pq,aa))
                t3 = threading.Thread(target=download1, args=(start,end,interval,q,pth,'1min',client3, pq,aa))
                t4 = threading.Thread(target=download1, args=(start,end,interval,q,pth,'1min',client4, pq,aa))
                t5 = threading.Thread(target=download1, args=(start,end,interval,q,pth,'1min',client5, pq,aa))
                t6 = threading.Thread(target=download1, args=(start,end,interval,q,pth,'1min',client6, pq,aa))
                t7 = threading.Thread(target=download1, args=(start,end,interval,q,pth,'1min',client7, pq,aa))
                t8 = threading.Thread(target=download1, args=(start,end,interval,q,pth,'1min',client8, pq,aa))
                t1.start()
                time.sleep(10)
                t2.start()
                time.sleep(10)
                t3.start()
                time.sleep(10)
                t4.start()
                time.sleep(10)
                t5.start()
                time.sleep(10)
                t6.start()
                time.sleep(10)
                t7.start()
                time.sleep(10)
                t8.start()
                t1.join()
                t2.join()
                t3.join()
                t4.join()
                t5.join()
                t6.join()
                t7.join()
                t8.join()
                
                
                
            
            
def startdownload_1min_nothread(name, withcgdata=True, withaggtrades=False, aggtradelimit=0, run=False, ver=None):
    if run == True:
        dirs = os.path.dirname(os.path.realpath(__file__))
        if withcgdata == True:
            pth = os.path.join(dirs, 'data-download/1min-unprocessed')
        else:
            pth = os.path.join(dirs, 'data-download/1min-processed')
        exchg = get_exchange_info()
        currlist = getstate(checkprog(pth),exchg)
        start = "5 Jan, 2021"
        end = "5 Jan, 2022"
        interval = Client.KLINE_INTERVAL_1MINUTE
        if not ver == None:
            currlist = takehalf(ver, currlist)
            download(start,end,interval,currlist,pth,'1min',withaggtrades=False,aggtradelimit=0)
        if ver == None:
            l1,l2,l3,l4,l5,l6,l7,l8 = splitlist(8,currlist)
        download1(start,end,interval,[name],pth,'1min',client8, 1,withaggtrades=False,aggtradelimit=0, )


def delete_incompletes(pth):
    def import_csv(csvfilename):
        data = []
        row_index = 0
        with open(csvfilename, "r", encoding="utf-8", errors="ignore") as scraped:
            reader = csv.reader(scraped, delimiter=',')
            for row in reader:
                if row:  # avoid blank lines
                    row_index += 1
                    columns = [str(row_index), row[0]]
                    data.append(columns)
        return data

    for a in os.listdir(pth):
        data = import_csv(os.path.join(pth,a))
        last_row = data[-1]
        if last_row[1] != '2022-01-04 23:59:00':
            print(a, ":", last_row[1])
            os.remove(os.path.join(pth,a))

    

def interval_to_milliseconds(interval):
    """Convert a Binance interval string to milliseconds
    :param interval: Binance interval string 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w
    :type interval: str
    :return:
         None if unit not one of m, h, d or w
         None if string not in correct format
         int value of interval in milliseconds
    """
    ms = None
    seconds_per_unit = {
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60
    }

    unit = interval[-1]
    if unit in seconds_per_unit:
        try:
            ms = int(interval[:-1]) * seconds_per_unit[unit] * 1000
        except ValueError:
            pass
    return ms

# requires dateparser package



startdownload_day(withcgdata=True, run=True)
# if __name__ == '__main__':
#     #fire.Fire(startdownload_day, command=('withcgdata=True','run= True'))  
#     fire.Fire({"make_dirs": make_dirs, "download_day": startdownload_day,"download_1min": startdownload_1min, "del_incomp": delete_incompletes})