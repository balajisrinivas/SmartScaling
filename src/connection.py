from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel, OperationTimedOut
from cassandra.query import SimpleStatement
from cassandra.query import BatchStatement

import monitoringClient

import time, threading
import logging
import socket
from time import sleep
from multiprocessing.queues import Queue
import random
from random import seed
import cassandra

log = logging.getLogger()
log.setLevel('INFO')

batchQ = Queue()
responseTimesQ = Queue()
finished_preparing_batches = False

g_monitorClient = monitoringClient.MonitoringClient()
g_rwLock = threading.Lock()

TOTAL_TIME =  1*60

class myThread (threading.Thread):
    def __init__(self, session, batch, threadID, whichFunc, total_requests):
        threading.Thread.__init__(self)
        self.session = session
        self.batch  = batch
        self.threadID = threadID
        self.whichFunc = whichFunc
        self.total_requests = total_requests
        self.num_requests_per_sec = total_requests/TOTAL_TIME
        #self.how_manu_at_a_time = 3000
        
    def run(self):
        if self.whichFunc == 0:
            prepareBatches(self.session, self.batch, self.total_requests)
        elif self.whichFunc == 1:
            sendData(self.session, self.batch, self.threadID, self.num_requests_per_sec)
        else:
            getData(self.session, self.batch, self.threadID, self.num_requests_per_sec)
            
def sendData(session, batchSize, threadID, num_requests_per_sec):
    
    global responseTimesQ
    isinvalid = ""
    
    keyspace = "balajiSpace"
    replication = "3"
    tableName = "expData"    
    
    batch_prep_start = time.time()
    
    insertQuery = session.prepare("""INSERT INTO """+keyspace+"""."""+tableName+""" (id, company, open, high, low, close) VALUES (?,?,?,?,?,?) ;""")
    batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
    
    #num1 = random.uniform(1,5555555)
    #num2 = random.uniform(2222,8888888)
    
    company = "msft"
    openVal = str(80)
    highVal = str(90)
    lowVal =  str(100)
    closeVal = str(110)
    
    for i in range(1, batchSize+1):
        s_date = str(num_requests_per_sec) + '_' + str(threadID) + '_' + str(i)+'_'+'12022014'
        
        global idVal
        idVal = s_date
    
        batch.add(insertQuery, (idVal, company, openVal, highVal, lowVal, closeVal))
    
    batch_prep_end = time.time()
    
    #print "Batch Prep time: ", (batch_prep_end - batch_prep_start)
    
    start = time.time()
    
    try:
        session.execute(batch)
        #sleep(1)
    except Exception as e:
        print e
        isinvalid = "invalid;"
        
    #sleep(1)
    end = time.time()
    
    output_str = isinvalid + "Writes;ThreadID: " + str(threadID) + ";Response Time: " + str(end - start) + "\n"
    
    responseTimesQ.put_nowait(output_str)
    #murali : send "write,1" to main.py 
    g_rwLock.acquire()
    g_monitorClient.sendMessage("write")
    g_rwLock.release()
    
def getData(session, batchSize, threadID, num_requests_per_sec):
    
    global responseTimesQ
    isinvalid = ""
    
    keyspace = "balajiSpace"
    replication = "3"
    tableName = "expData"
    
    start_id = str(num_requests_per_sec) + "_" + str(threadID) + "_1"+'_'+'12022014'
    end_id = str(num_requests_per_sec) + "_" + str(threadID) + "_500"+'_'+'12022014'
    
    results = 0
    
    #print batchSize
    
    start = time.time()

    #murali : send "read,1" to main.py 
    g_rwLock.acquire()
    g_monitorClient.sendMessage("read")
    g_rwLock.release()

    
    #results = session.execute("""SELECT * FROM """+keyspace+"""."""+tableName+""" WHERE open >= '""" + start_id +"""' AND open <= '""" + end_id +"""' LIMIT """ + str(batchSize) + """;""")
    #results = session.execute("""SELECT * FROM """+keyspace+"""."""+tableName+""" WHERE token(id) > token('""" + start_id + """') AND token(id) < token('""" + end_id +"""') LIMIT """ + str(batchSize) + """;""")
    #results = session.execute("""SELECT * FROM """+keyspace+"""."""+tableName+""" WHERE token(id) > token('""" + start_id + """') LIMIT """ + str(batchSize) + """;""")
    
    try:
        results = session.execute("""SELECT * FROM """+keyspace+"""."""+tableName+""" WHERE token(id) > token('""" + start_id + """') LIMIT """ + str(batchSize) + """;""")
        #sleep(1)
    except Exception as e:
        print e
        isinvalid = "invalid;"
    
    end = time.time()
    
    #print results
    
    #print len(results), start_id, end_id, "\n"
    
    output_str = isinvalid + "Reads;ThreadID: " + str(threadID) + ";Response Time: " + str(end - start) + "\n"
    
    #if len(results) > 0:
    responseTimesQ.put_nowait(output_str)
    


class SimpleClient:
    
    def __init__(self):
        #self.mc = monitoringClient.MonitoringClient()
        self.session = None
        self.connList = [None, None, None, None, None, None, None, None, None, None]
        
    def __del__(self):
        pass

    def connect(self, nodes):
        print nodes
        cluster = Cluster(nodes)
        metadata = cluster.metadata	
		
        #for i in range(0, len(self.connList)):
            #self.connList[i] = cluster.connect()
            #print self.connList[i]
		
        cluster.protocol_version = 3
        #cluster.default_retry_policy = cassandra.policies.FallthroughRetryPolicy
        
        print "Maxconn local: ", cluster.get_max_connections_per_host(0)
        print "Maxconn remote: ", cluster.get_max_connections_per_host(1)
        
        self.session = cluster.connect()
        self.session.default_timeout = 120
        
        
        print('Connected to cluster: ' + metadata.cluster_name)
        
        for host in metadata.all_hosts():
            print('Datacenter: %s; Host: %s; Rack: %s', host.datacenter, host.address, host.rack)
        
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------
    def close(self):
        self.session.cluster.shutdown()
        self.session.shutdown()
        log.info('Connection closed.')
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------
    def create_schema(self, keyspace, replication):
        create_keyspace = """CREATE KEYSPACE """+keyspace+""" WITH replication = {'class':'SimpleStrategy', 'replication_factor':"""+replication+"""};"""
        self.session.execute(create_keyspace)
        print('Keyspace created.')
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------
    def create_column_family(self, keyspace, tableName):
        create_column_family = """CREATE TABLE """+keyspace+"""."""+tableName+""" (id text PRIMARY KEY, company text, open text, high text, low text, close text) WITH caching = 'none';"""	
        self.session.execute(create_column_family)
        print("Column family created.")
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------
    def load_data(self, keyspace, tableName, how_many_at_a_time, sleep_interval, num_requests_per_sec, seekPos):
        
        global responseTimesQ, finished_preparing_batches, batchQ
        
        #curThread = myThread(self.session, how_many_at_a_time, None, 0, (num_requests_per_sec * 60))
                                
        #curThread.start()       
        
        threadID = 1
        
        #sleep(10)
        
        #total_sleep_time = TOTAL_TIME * 1000
        curSlept = 1
        
        threads = list()
        
        start_time = time.time()
        
        cumulative_file_read_time = 0
        
        canExit = False
        
        while 1:
            
            for i in range(1, num_requests_per_sec + 1):
                
                #if batchQ.qsize() <= num_requests_per_sec:
                #    break
                
                #batch = batchQ.get_nowait()
                #mysession = random.choice(self.connList)
                curThread = myThread(self.session, how_many_at_a_time, threadID, 1, (num_requests_per_sec * TOTAL_TIME))
                                
                curThread.start()
                
                threads.append(curThread)
               
                threadID += 1#inside outer for loop
             
            #if batchQ.qsize() == 0 and finished_preparing_batches == True:
            #    canExit = True
            #    break
            
            #print "out of outer for loop"
            
            '''if canExit:
                print "outer while loop: finished creating threads"
                break'''
            
            if curSlept >= TOTAL_TIME:
                print "sleep", curSlept, TOTAL_TIME
                #print "slept for long enough"
                
                break
            
            #print "sleeping..." 
            #if curSlept < total_sleep_time:
            
            "sleep only if batches are still being prepared"
            '''if not finished_preparing_batches:
                sleep(1)
                curSlept += (1*1000)'''     
            sleep(1)
            curSlept += 1
        
        end_time = time.time()
        
        print "NumRequests Per Second:", num_requests_per_sec
        print "Total Requests:", (threadID - 1)        
        
        # Wait for all threads to complete
        for t in threads:
            t.join()     
            
        threads_finish_time = time.time()       
        
        print "Total threads fireup duration: ", (end_time - start_time)
         
        print "All threads finish time: ", (threads_finish_time - start_time)
        #out_file.close()
        
        respFile = str(num_requests_per_sec) + "w.txt"
        respTimeFile = open(respFile, "w")
        respTimeFile.seek(0)
        
        respTimeFile.write("Total threads duration:" + str(end_time - start_time) + "\n")
        respTimeFile.write("rtq:" + str(responseTimesQ.qsize()) + ";threads:" + str(len(threads)) + "\n")
        
        
        counter = responseTimesQ.qsize()
        for i in range(1, counter+1):
            respTimeFile.write(responseTimesQ.get_nowait())       
        
            
        respTimeFile.close()
        
        #total_function_time = (end_time - start_time)       
        

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------
    def getRowKey(self, rowNumber):
        date = "20141108"
        hours = 0
        minutes = 0
        seconds = 0
        mseconds = 0
        for i in range(rowNumber):
            mseconds += 1
            if mseconds == 150:
                seconds += 1
                mseconds = 0
            if seconds == 60:
                minutes += 1
                seconds = 0
            if minutes == 60:
                hours += 1
                minutes = 0
        print "hours:"+str(hours)+"mins:"+str(minutes)+"secs:"+str(seconds)+"msecs:"+str(mseconds)
    
    def getAndWrite(self, keyspace, tableName, how_many_at_a_time, sleep_interval, num_requests_per_sec, seekPos):
        global responseTimesQ, finished_preparing_batches, batchQ
        
        threadID = 1
        curSlept = 1
        
        threads = list()
        
        start_time = time.time()
        
        cumulative_file_read_time = 0
        
        canExit = False        
        
        temp = int(num_requests_per_sec * 0.5)
        
        if temp > 0:
            how_many_writes = temp
        else:
            how_many_writes = num_requests_per_sec
        
        while 1:
            
            for i in range(1, num_requests_per_sec + 1):
                
                if i < how_many_writes + 1:
                    curThread = myThread(self.session, how_many_at_a_time, threadID, 1, (num_requests_per_sec * TOTAL_TIME))
                else:
                    curThread = myThread(self.session, how_many_at_a_time, threadID, 2, (num_requests_per_sec * TOTAL_TIME))
                                
                curThread.start()
                
                threads.append(curThread)
               
                threadID += 1#inside outer for loop
             
            if curSlept >= TOTAL_TIME:
                print "sleep", curSlept, TOTAL_TIME
                #print "slept for long enough"
                
                break
            
            sleep(1)
            curSlept += 1
        
        end_time = time.time()
        
        print "NumRequests Per Second:", num_requests_per_sec
        print "Total Requests:", (threadID - 1)        
        
        # Wait for all threads to complete
        for t in threads:
            t.join()     
            
        threads_finish_time = time.time()       
        
        print "Total threads fireup duration: ", (end_time - start_time)
         
        print "All threads finish time: ", (threads_finish_time - start_time)
        #out_file.close()
        
        respFile = str(num_requests_per_sec) + "rw.txt"
        respTimeFile = open(respFile, "w")
        respTimeFile.seek(0)
        
        respTimeFile.write("Total threads duration:" + str(end_time - start_time) + "\n")
        respTimeFile.write("rtq:" + str(responseTimesQ.qsize()) + ";threads:" + str(len(threads)) + "\n")
        
        
        counter = responseTimesQ.qsize()
        for i in range(1, counter+1):
            respTimeFile.write(responseTimesQ.get_nowait())       
        
            
        respTimeFile.close()
    
    
    def query_schema(self, keyspace, tableName, how_many_at_a_time, sleep_interval, num_requests_per_sec, seekPos):
        global responseTimesQ, finished_preparing_batches, batchQ
        
        threadID = 1
        curSlept = 1
        
        threads = list()
        
        start_time = time.time()
        
        cumulative_file_read_time = 0
        
        canExit = False
        
        while 1:
            
            
            
            for i in range(1, num_requests_per_sec + 1):
                curThread = myThread(self.session, how_many_at_a_time, threadID, 2, (num_requests_per_sec * TOTAL_TIME))
                                
                curThread.start()
                
                threads.append(curThread)
               
                threadID += 1#inside outer for loop
             
            if curSlept >= TOTAL_TIME:
                print "sleep", curSlept, TOTAL_TIME
                #print "slept for long enough"
                
                break
            
            sleep(1)
            curSlept += 1
        
        end_time = time.time()
        
        print "NumRequests Per Second:", num_requests_per_sec
        print "Total Requests:", (threadID - 1)        
        
        # Wait for all threads to complete
        for t in threads:
            t.join()     
            
        threads_finish_time = time.time()       
        
        print "Total threads fireup duration: ", (end_time - start_time)
         
        print "All threads finish time: ", (threads_finish_time - start_time)
        #out_file.close()
        
        respFile = str(num_requests_per_sec) + "r.txt"
        respTimeFile = open(respFile, "w")
        respTimeFile.seek(0)
        
        respTimeFile.write("Total threads duration:" + str(end_time - start_time) + "\n")
        respTimeFile.write("rtq:" + str(responseTimesQ.qsize()) + ";threads:" + str(len(threads)) + "\n")
        
        
        counter = responseTimesQ.qsize()
        for i in range(1, counter+1):
            respTimeFile.write(responseTimesQ.get_nowait())
        
            
        respTimeFile.close()
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------

