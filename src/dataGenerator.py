import random
from random import seed

if __name__ == '__main__':
    inputFileName = "input.txt"
    f1 = open(inputFileName,'r')
    stockSymbolList = []
    dataList=[]
    for line in f1:
        line = line.rstrip('\n')
        if line.startswith("date"):
            flag1 = 1
        elif line.startswith("stock_symbols"):
            flag2 = 1
            flag1 = 0
        elif line == '':
            pass
        elif flag1 == 1:
            date=int(line)
        else:
            stockSymbolList.append(line)
    outputFileName = "output.txt"
    f2 = open(outputFileName,"w")
    f2.seek(0)
    #print date
    #print stockSymbolList
    numHours = 12
    numMinutes = 24
    numSeconds = 24
    numMilliseconds = 30
    lenStockSymbolList = len(stockSymbolList)
    random.seed(random.randint(100,200))
    for i in range(numHours):
        for j in range(numMinutes):
            for k in range(numSeconds):
                for l in range(numMilliseconds):
                    tdata=""
                    tdata+=str(date)+str(i).zfill(2)+str(j).zfill(2)+str(k).zfill(2)+"."+str(l).zfill(2)
                    #print tdata
                    tdata += ','
                    stockSymbolListIndex = random.randrange(0,lenStockSymbolList,1)
                    tdata += stockSymbolList[stockSymbolListIndex]
                    tdata += ','
                    open = random.uniform(30,100)
                    low = open - random.uniform(0,20)
                    high = open + random.uniform(0,20)
                    if(random.random() < 0.5):
                        close = (low + open)/2
                    else:
                        close = (open + high)/2
                    tdata += str(open)+','+str(high)+','+str(low)+','+str(close)
                    tdata +='\n'
                    print tdata
                    f2.write(tdata)
    f1.close()
    f2.close()