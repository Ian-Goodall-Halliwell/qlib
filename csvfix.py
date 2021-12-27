import csv
import os
from pycoingecko import CoinGeckoAPI
import time
cg = CoinGeckoAPI()
tokid = {}
aa = cg.get_coins_list(include_platform='false')
for a in os.listdir("C:/Users/gooda/Documents/GitHub/qlib/binancedata_red"):
    b = a.split('.')[0].split('BUSD')[0].lower()
    for tokn in aa:
        if tokn['symbol'] == b:
            tokid[a] = tokn['id']

    #with open(os.path.join ("C:/Users/gooda/Documents/GitHub/qlib/binancedata_red", a), 'r', newline='') as read_csv:




for curtok in tokid:
    initdate = 0
    cnt = 0
    it = 0
    with open(os.path.join("C:/Users/gooda/Documents/GitHub/qlib/binancedata_red", curtok)) as rc, open(os.path.join("C:/Users/gooda/Documents/GitHub/qlib/output", curtok), 'w', newline='') as wc:
        for enum, line in enumerate(csv.DictReader(rc)):
            
            it=it+1
            vdate = line['date'].split(' ')[0]
            if vdate != initdate:
                if enum == 0:
                    tic = time.perf_counter()
                cnt = cnt+1
                initdate = line['date'].split(' ')[0]
                dlist = initdate.split('-')
                initdate1 = str(dlist[2] + "-" + dlist[1] + "-" + dlist[0])
                try:
                    mcpp = cg.get_coin_history_by_id(tokid[curtok],initdate1)
                except:
                    print('error grabbing')
                    time.sleep(60)
                    mcpp = cg.get_coin_history_by_id(tokid[curtok],initdate1)
                if not 'market_data' in mcpp:
                    tpfact = 1
                    continue
                mcp = mcpp['market_data']['market_cap']['usd']
                curprice = mcpp['market_data']['current_price']['usd']
                if enum == 0:
                    BASEfact = mcp/curprice
            
                CURRfact = mcp/float(line['close'])
                
                if CURRfact == 0:
                    CURRfact = BASEfact
                if CURRfact == 0.0:
                    tpfact = 1
                else:
                    tpfact = BASEfact/CURRfact
                
                if cnt == 45:
                    tm = 62 - (time.perf_counter() - tic)
                    print(abs(tm))
                    time.sleep(abs(tm))
                    print(curtok, ' iternum {}'.format(it))
                    cnt = 0
                    tic = time.perf_counter()

            line['factor'] = tpfact
            wrt = csv.DictWriter(wc, line.keys())
            if enum == 0:
                wrh = csv.writer(wc)
                wrh.writerow(line.keys())
            wrt.writerow(line)
            



# for a  in os.listdir("C:/Users/gooda/Documents/GitHub/qlib/binancedata_red"):
#     with open(os.path.join ("C:/Users/gooda/Documents/GitHub/qlib/binancedata_red", a), 'r', newline='') as ready, open(os.path.join ("C:/Users/gooda/Documents/GitHub/qlib/outdata", a), 'w', newline='') as writ:
#              remf = csv.writer(writ)
#              for enum, rr in enumerate(csv.reader(ready)):
#                 if enum == 0:
#                     rr.append('factor')
#                     remf.writerow(rr)
#                 else:
#                     rr.append('1')
#                     remf.writerow(rr)
                