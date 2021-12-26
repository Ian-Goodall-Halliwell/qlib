import csv
import os
for a  in os.listdir("C:/Users/gooda/Documents/GitHub/qlib/binancedata_red"):
    with open(os.path.join ("C:/Users/gooda/Documents/GitHub/qlib/binancedata_red", a), 'r', newline='') as ready, open(os.path.join ("C:/Users/gooda/Documents/GitHub/qlib/outdata", a), 'w', newline='') as writ:
             remf = csv.writer(writ)
             for enum, rr in enumerate(csv.reader(ready)):
                if enum == 0:
                    rr.append('factor')
                    remf.writerow(rr)
                else:
                    rr.append('1')
                    remf.writerow(rr)
                