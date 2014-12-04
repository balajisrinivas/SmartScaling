import random
from random import seed

def main():
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
    numHours = 24
    numMinutes = 60
    numSeconds = 60
    numMilliseconds = 1000
    lenStockSymbolList = len(stockSymbolList)
    random.seed(random.randint(100,200))
    
    NUM_RECORDS = 4000000
    NUM_RECORDS_PER_SEC = 500
    count = 0
    total_records = 0
    
    for i in range(numHours):
        for j in range(numMinutes):
            for k in range(numSeconds):
                for l in range(numMilliseconds):
                    tdata=""
                    tdata+=str(date)+str(i).zfill(2)+str(j).zfill(2)+str(k).zfill(2)+"."+str(l).zfill(3)
                    #print tdata
                    tdata += ','
                    stockSymbolListIndex = random.randrange(0,lenStockSymbolList,1)
                    tdata += stockSymbolList[stockSymbolListIndex]
                    tdata += ','
                    op = random.uniform(30,100)
                    low = op - random.uniform(0,20)
                    high = op + random.uniform(0,20)
                    if(random.random() < 0.5):
                        close = (low + op)/2
                    else:
                        close = (op + high)/2
                    tdata += str(op)+','+str(high)+','+str(low)+','+str(close)
                    tdata +='\n'
                    #print tdata
                    f2.write(tdata)
                    count += 1
                    total_records += 1                   
                    
                    if total_records == NUM_RECORDS:
                        f1.close()
                        f2.close()
                        return
                    
                #if count == NUM_RECORDS_PER_SEC:
                #    break

if __name__ == '__main__':
    main()
    