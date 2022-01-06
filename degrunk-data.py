import re
from binance.client import Client
import time
import csv
import os
import fire
client = Client('igEARWI7LNtjhzHa3zrNAMtLlLtUjnNb3VFHSHCf5Nlnga4h3vAzthAQKe8wLYlC', 'BM8EVK6TI5kHKQ7sORXpkwHet8mtq8alhOV5JJQ25kAIunKL7YkGgfc80inJad0I')
from pycoingecko import CoinGeckoAPI
import json
from binance.client import Client
import binance.enums
from datetime import date
import dateparser
import pytz
from datetime import datetime

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

    # with open('dltracker.csv', 'r') as rin,open('newtemp.csv', 'w') as rem:
    #     remf = csv.writer(rem)
    #     for rr in csv.reader(rin):
    #         if not token in rr:
    #             remf.writerow(rr)
    # os.remove('dltracker.csv')
    # os.rename('newtemp.csv', 'dltracker.csv')
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
    dd =  client.get_exchange_info()
    return dd
def newest(path):
    files = os.listdir(path)
    paths = [os.path.join(path, basename) for basename in files]
    try:
        max(paths, key=os.path.getctime)
    except:
        print('skippin')
    return 

def checkprog(pth):
    outlist = []
    qqq = os.listdir(pth)
    a = newest(pth)
    if a != None:
        os.remove(a)
    qqq = os.listdir(pth)
    for qqi in qqq:
        qqi = qqi.split('.')[0]
        outlist.append(qqi)
    
    return outlist

def getstate(outlist, exchangeinfo, tokenname = 'USDT'):
    currlist = []
    for ab in exchangeinfo['symbols']:
        if ab['quoteAsset'] == tokenname:
            print(ab['symbol'])
            if not ab['symbol'] in outlist:
                currlist.append(ab['symbol'])
    return currlist

def download(start, end, interval, currlist, pth, type, withcgdata=True, withaggtrades=False, aggtradelimit=0):    
    from binance.enums import HistoricalKlinesType
    if withcgdata == True:
        cg = CoinGeckoAPI()
        aa = cg.get_coins_list()
    for token in currlist:
        if withcgdata == True:
            ccdcdc = date_to_milliseconds(start)//1000 #+ 86400
            ccdcdd = date_to_milliseconds(end)//1000 + 3600
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
                    
                    #print('skipping', token)
                    
             
                
            
            # +
            #     try:
            #         print('request failed, waiting 60s')
                    
            #         cval = cg.get_coin_market_chart_range_by_id(id=tokid[token], vs_currency="usd", from_timestamp=ccdcdc, to_timestamp=ccdcdd)
            #     except:
            #         try:
            #             cval = cg.get_coin_market_chart_range_by_id(id=b, vs_currency="usd", from_timestamp=ccdcdc, to_timestamp=ccdcdd)
            #         except:
        klines = client.get_historical_klines(token, interval, start, end,  klines_type= HistoricalKlinesType.SPOT)
                    
        # try:    
        #     klines = client.get_historical_klines(token, interval, ccdcdc, ccdccc,  klines_type= HistoricalKlinesType.SPOT)
        # except:
        #     print('waiting on ', token)
        #     time.sleep(60)
        #     klines = client.get_historical_klines(token, interval, ccdcdc, ccdccc,  klines_type= HistoricalKlinesType.SPOT)

        if not klines == []:
            klines.pop(-1)
        else:
            continue
        if len(cval['market_caps']) != 0:
            d1 = [item[1] for item in cval['market_caps']]
            # if type == 'day': 
            #     clis = []
            #     ct = 0
            #     cnt = 0
            #     for v in d1:
            #         cnt = cnt + 1
            #         ct = ct + v
            #         if cnt == 24:
            #             ct = ct/24
            #             clis.append(ct)
            #             ct = 0
            #     d1 = clis
        else:
            d1 = []
            d1.extend([0] * len(klines))
        if len(cval['total_volumes']) != 0:
            d2 = [item[1] for item in cval['total_volumes']]
            # if type == 'day': 
            #     clis = []
            #     ct = 0
            #     cnt = 0
            #     for v in d2:
            #         cnt = cnt + 1
            #         ct = ct + v
            #         if cnt == 24:
            #             clis.append(ct)
            #             ct = 0
            #     d2 = clis
        else: 
            d2 = []
            d2.extend([0] * len(klines))
        if len(cval['prices']) != 0:
            d3 = [item[1] for item in cval['prices']]
            # if type == 'day':
            #     clis = []
            #     ct = 0
            #     cnt = 0
            #     for v in d3:
            #         cnt = cnt + 1
            #         ct = ct + v
            #         if cnt == 24:
            #             ct = ct/24
            #             clis.append(ct)
            #             ct = 0
            #     d3 = clis
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
                            print('{} Iteration #: '.format(token), it)
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
                            print('{} Iteration #: '.format(token), it)
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
                        print('{} Iteration #: '.format(token), it)
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

def startdownload_day(withcgdata=True, withaggtrades=False, aggtradelimit=0, run=True, ver=None):
    if run == True:
        dirs = os.path.dirname(os.path.realpath(__file__))
        if withcgdata == True:
            pth = os.path.join(dirs, 'data-download/day-unprocessed')
        else:
            pth = os.path.join(dirs, 'data-download/day-processed')
        exchg = get_exchange_info()
        currlist = getstate(checkprog(pth),exchg)
        if not ver == None:
            currlist = takehalf(ver, currlist)

        start = "21 Dec, 2010"
        end = "5 Jan, 2022"
        interval = Client.KLINE_INTERVAL_1DAY
        download(start,end,interval,currlist,pth,'day',withaggtrades=False,aggtradelimit=0)

def startdownload_1min(withcgdata=True, withaggtrades=False, aggtradelimit=0, run=False, ver=None):
    if run == True:
        dirs = os.path.dirname(os.path.realpath(__file__))
        if withcgdata == True:
            pth = os.path.join(dirs, 'data-download/1min-unprocessed')
        else:
            pth = os.path.join(dirs, 'data-download/1min-processed')
        exchg = get_exchange_info()
        currlist = getstate(checkprog(pth),exchg)

        if not ver == None:
            currlist = takehalf(ver, currlist)

        start = "21 Dec, 2010 0:00"
        end = "5 Jan, 2022 0:00"
        interval = Client.KLINE_INTERVAL_1MINUTE
        download(start,end,interval,currlist,pth,'1min',withaggtrades=withaggtrades,aggtradelimit=aggtradelimit)


    

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




if __name__ == '__main__':
    #fire.Fire(startdownload_day)  
    fire.Fire({"make_dirs": make_dirs, "download_day": startdownload_day,"download_1min": startdownload_1min})