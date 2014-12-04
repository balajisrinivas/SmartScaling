import sys
import socket, threading, time, string
from constants import *
from time import sleep
from startOrStopCassandra import StartOrStop


g_startTime = 0
g_responseTime = 0
g_responseTimeSLA = 0
g_respTimeDelta = 0.5
g_numReadRequests = 0
g_numWriteRequests = 0
g_maxRequestRate = 0.0

"Object to start or stop cassandra node"
cassandra = StartOrStop()

"Request rates dictionary"
g_readRR = dict()
g_writeRR = dict()
g_rwRR = dict()#Read/Write

g_curReqRate = 0
g_curNumServers = 10

g_startedGettingData = False

'''This is used as program flow control;
# 1--> calculate threshold; 
# 2--> watch for brining down servers
# 3--> watch for bringing up servers
'''
g_whatToMonitor = 1

g_rwLock = threading.Lock()

class myThread (threading.Thread):
    def __init__(self, connSocket, clientAddress, isMonitor=False):
        threading.Thread.__init__(self)
        self.connSocket = connSocket
        self.clientAddress = clientAddress
        self.isMonitorThread = isMonitor
        
    def run(self):
        if self.isMonitorThread:
            monitorResponseTime()
        else:
            connectionHandler(self.connSocket, self.clientAddress)

def main():
    "Fill RequestRate Vs Servers dictionaries"
    g_readRR[10] = 24.978
    g_readRR[9] = 24.283
    g_readRR[8] = 24.262
    g_readRR[7] = 24.250
    g_readRR[6] = 24.065
    g_readRR[5] = 23.138

    g_rwRR[10] = 19.230
    g_rwRR[9] = 19.602
    g_rwRR[8] = 19.671
    g_rwRR[7] = 19.656
    g_rwRR[6] = 19.133
    g_rwRR[5] = 16.735

    g_writeRR[10] = 16.381
    g_writeRR[9] = 16.06
    g_writeRR[8] = 15.96
    g_writeRR[7] = 15.890
    g_writeRR[6] = 15.33

    
    #create an INET, STREAMing socket
    listenSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    listenSocket.bind(('', LISTEN_PORT))
    
    listenSocket.listen(BACKLOG_QUEUE)

    isStartTimer = True
    
    monThread = myThread(None, None, True)
    monThread.start()
    
    while 1:
        #accept connections from outside
        (clientsocket, address) = listenSocket.accept()
        
        cliThread = myThread(clientsocket, address, False)
        cliThread.start()
        #thread.start_new_thread(connectionHandler, (clientsocket, address))

def connectionHandler(connSocket, address):
    global g_numReadRequests, g_numWriteRequests, g_startedGettingData
    while 1:
	
        #g_rwLock.acquire()
        buffer = connSocket.recv(MAX_READ_LEN)
        #g_rwLock.release()
        
        "Connection has been terminated by client"
        if buffer == '':
            break
        
#        print buffer
        
        g_startedGettingData = True
        
        tokens = string.split(buffer, ",")        
        
        g_rwLock.acquire()
	count = tokens[0].count("write")
        if count > 0:
            g_numWriteRequests += count
        
	count = tokens[0].count("read")
        if count > 0:
            g_numReadRequests += count
            
        #if tokens[0] == "readwrite":
        #    g_numReadRequests += int(tokens[1])
        #    g_numWriteRequests += int(tokens[2])
            
        #g_responseTime = responseTime
        g_rwLock.release()
        

def decideForReadServer(requestRate):

    maxRR = [24.978, 24.283, 24.262, 24.250, 24.065, 23.138]#must be in decreasing order
    
    index = None
    prevKey = None

    for i in range(0, len(maxRR)):
        if requestRate < maxRR[i]:
            continue
        
	if i == 0:
	    prevKey = maxRR[0]
	    break

	prevKey = maxRR[i-1]
	break

    if prevKey == None:
	prevKey = maxRR[len(maxRR)-1]

    for key in g_readRR.keys():
        if g_readRR[key] == prevKey:
            index = key
            break

    if index == None:
        return None

    print "Current Num Servers: ", g_curNumServers, "; New Num Servers: ", index
            
    if g_curNumServers < index:
        return ["U", index]
    elif g_curNumServers > index:
        return ["D", index]
    else:
        return ["N", index]
    
def decideForWriteServer(requestRate):
    
    maxWR = [16.381, 16.06, 15.96, 15.890, 15.33]#must be in decreasing order
    
    index = None
    prevKey = None

    for i in range(0, len(maxWR)):
        if requestRate < maxWR[i]:
            continue
        
	if i == 0:
	    prevKey = maxWR[0]
	    break

	prevKey = maxWR[i-1]
	break

    if prevKey == None:
	prevKey = maxWR[len(maxWR)-1]

    for key in g_writeRR.keys():
        if g_writeRR[key] == prevKey:
            index = key
            break

    if index == None:
        return None

    print "Current Num Servers: ", g_curNumServers, "; New Num Servers: ", index
            
    if g_curNumServers < index:
        return ["U", index]
    elif g_curNumServers > index:
        return ["D", index]
    else:
        return ["N", index]

def decideForReadWriteServer(requestRate):
    
    maxRWR = [21]#must be in decreasing order
    
    index = 0
    
    for i in range(0, len(g_rwRR)):
        if requestRate < g_rwRR[i]:
            continue
        
        index = i-1
	break

    print "Current Num Servers: ", g_curNumServers, "; New Num Servers: ", g_rwRR[index]
        
    if g_curNumServers < g_rwRR[index]:
        return ["U", g_rwRR[index]]
    elif g_curNumServers > g_rwRR[index]:
        return ["D", g_rwRR[index]]
    else:
        return ["N", g_rwRR[index]]
    
def monitorResponseTime():
    print "Monitors change of state every 2 minute"
    
    global g_maxRequestRate, g_startedGettingData, g_whatToMonitor, g_curNumServers, g_curReqRate
    global g_numReadRequests, g_numWriteRequests
    
    while 1:
        if g_startedGettingData:
            break
        else:
            sleep(1)
    print "Out of first while loop"

    g_whatToMonitor = 2#Assuming cluster always starts with full capacity
    g_curReqRate =  24.978 #Start with Max req rate
    timeToSleep = 2 * 60
    g_curNumServers = 10 #Start with max capacity    
    reqType = "read"
    
    
    while 1:
        sleep(timeToSleep)
        print "Out of sleep"          
#        g_rwLock.acquire()
        
        curRequestRate = (g_numReadRequests + g_numWriteRequests) / float(timeToSleep)
        print "Current Request rate:", curRequestRate

#        g_rwLock.release()
        
        if reqType == "read":
            retVal = decideForReadServer(curRequestRate)
            
        if reqType == "write":
            retVal = decideForWriteServer(curRequestRate)
            
        if reqType == "readwrite":
            retVal = decideForReadWriteServer(curRequestRate)

        if retVal == None:
           print "going to sleep"
           continue
        
        print retVal, g_curNumServers

        if int(retVal[1]) == g_curNumServers:
	   g_numReadRequests = 0
	   g_numWriteRequests = 0
           continue

        if retVal[0] == "U":
            print "increase servers", retVal, g_curNumServers
            for i in range(0, (int(retVal[1]) - int(g_curNumServers))):
		#bring up servers
		cassandra.startCassandra()
            g_startedGettingData = False
            
        if retVal[0] == "D":
            print "decrease servers", retVal, g_curNumServers
            for i in range(0, (int(g_curNumServers) - int(retVal[1]))):
                #bring down servers
		cassandra.stopCassandra()
            g_startedGettingData = False

        g_curNumServers = int(retVal[1])
	g_numReadRequests = 0
	g_numWriteRequests = 0

        while 1:
        	if g_startedGettingData:
        	    break
        	else:
            	    sleep(1)
                
        "sleep for 1 min"
        #g_startTime = time.time()
        #sleep(timeToSleep)  
	#print "Out of sleep"          
    
if __name__ == "__main__":
    main()
