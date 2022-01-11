import os
import csv

type = 'day'

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
def degunk():
    if type == '1min':
        for a in os.listdir("F:/q-bin/data-download/1min-unprocessed"):
            data = import_csv(os.path.join("F:/q-bin/data-download/1min-unprocessed",a))
            if data == []:
                print(a, ": Null")
                os.remove(os.path.join("F:/q-bin/data-download/1min-unprocessed",a))
                continue
            last_row = data[-1]
            if last_row[1] != '2022-01-04 23:59:00':
                print(a, ":", last_row[1])
                os.remove(os.path.join("F:/q-bin/data-download/1min-unprocessed",a))
    if type == 'day':
        for a in os.listdir("F:/q-bin/data-download/day-unprocessed"):
            data = import_csv(os.path.join("F:/q-bin/data-download/day-unprocessed",a))
            if data == []:
                print(a, ": Null")
                os.remove(os.path.join("F:/q-bin/data-download/day-unprocessed",a))
                continue
            last_row = data[-1]
            if last_row[1] != '2022-01-04':
                print(a, ":", last_row[1])
                os.remove(os.path.join("F:/q-bin/data-download/day-unprocessed",a))

def makesame():
    alist = []
    for a in os.listdir("F:/q-bin/data-download/1min-unprocessed"):
        alist.append(a)
    blist = []
    for b in os.listdir("F:/q-bin/data-download/day-unprocessed"):
        blist.append(b)
    for elem in alist:
        if not elem in blist:
            print('a',elem)
    for elmt in blist:
        if not elmt in alist:
            print('b',elmt)    
            os.remove(os.path.join("F:/q-bin/data-download/day-unprocessed",elmt))
#degunk()
makesame()
