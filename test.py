from binance.client import Client
import time
import csv
import os
client = Client('igEARWI7LNtjhzHa3zrNAMtLlLtUjnNb3VFHSHCf5Nlnga4h3vAzthAQKe8wLYlC', 'BM8EVK6TI5kHKQ7sORXpkwHet8mtq8alhOV5JJQ25kAIunKL7YkGgfc80inJad0I')

import json
from binance.client import Client


outlist = []
qqq = os.listdir('C:/Users/gooda/Documents/GitHub/qlib/minCSV')
for qqi in qqq:
    qqi = qqi.split('.')[0]
    outlist.append(qqi)
#currlist=outlist

firsttime = True
if firsttime == True:
    dd =  client.get_exchange_info()
    currlist = []
    for ab in dd['symbols']:
        if ab['quoteAsset'] == 'BUSD':
            print(ab['symbol'])
            if not ab['symbol'] in outlist:
                currlist.append(ab['symbol'])
    with open('dltracker.csv', 'w', newline='') as t:
        wr = csv.writer(t,delimiter=',')
        for en, aaaa in enumerate(currlist):
            wr.writerow([currlist[en]])
    currlist = []
    with open('dltracker.csv', newline='') as dlf:
            wrf = csv.reader(dlf, delimiter=',')
            for row in wrf:
                row = row[0].strip()
                currlist.append(row)

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

start = "21 Dec, 2019"
end = "21 Dec, 2021"
interval = Client.KLINE_INTERVAL_1DAY

for token in currlist:
    klines = client.get_historical_klines_generator(token, interval, start, end)
    print(token)



    with open('C:/Users/gooda/Documents/GitHub/qlib/minCSV/{}.csv'.format(token), 'w', newline='') as f:
        writerc = csv.writer(f)
        dic = ['date', 'open', 'high','low', 'close','volume','symbol','QAV','numberoftrades','takerbuyBAV','takerbuyQAV']
        writerc.writerow(dic)
        gct = 0
        it = 0
        for a in klines:
            
            #dd = datetime.fromtimestamp(a[0]/1000.0,tz=pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
            dd = datetime.fromtimestamp(a[0]/1000.0,tz=pytz.utc).strftime("%Y-%m-%d")
            b = [dd, a[1], a[2], a[3], a[4], a[5],token,a[7],a[8],a[9],a[10]]
            writerc.writerow(b)
            gct = gct + 1
            if gct == 1000:
                it = it + 1
                gct= 0
                print('{} Iteration #: '.format(token), it)
    # with open('dltracker.csv', 'r') as rin,open('newtemp.csv', 'w') as rem:
    #     remf = csv.writer(rem)
    #     for rr in csv.reader(rin):
    #         if not token in rr:
    #             remf.writerow(rr)
    # os.remove('dltracker.csv')
    # os.rename('newtemp.csv', 'dltracker.csv')
                