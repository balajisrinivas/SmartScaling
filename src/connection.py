from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.query import BatchStatement

import time
import logging
import socket

log = logging.getLogger()
log.setLevel('INFO')

class SimpleClient:
    
    def __init__(self):
        self.session = None
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.hostname = "172.24.26.37"
        self.port = 13567
        self.s.connect((self.hostname, self.port))

    def __del__(self):
        self.s.close()

    def connect(self, nodes):
        cluster = Cluster(nodes)
        metadata = cluster.metadata
        self.session = cluster.connect()
        print('Connected to cluster: ' + metadata.cluster_name)

        for host in metadata.all_hosts():
            print('Datacenter: %s; Host: %s; Rack: %s', host.datacenter, host.address, host.rack)
        self.s.send("Balaji");
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
        create_column_family = """CREATE TABLE """+keyspace+"""."""+tableName+""" (id text PRIMARY KEY, company text, open text, high text, low text, close text);"""	
        self.session.execute(create_column_family)
        print("Column family created.")
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------
    def load_data(self, keyspace, tableName, how_many_at_a_time, sleep_interval):
        out_file = open('output.txt','r')
        data = out_file.readlines()
        insertQuery = self.session.prepare("""INSERT INTO """+keyspace+"""."""+tableName+""" (id, company, open, high, low, close) VALUES (?,?,?,?,?,?) ;""")
        batch = BatchStatement(consistency_level=ConsistencyLevel.ANY)
        counter = 0
        for line in data:
            counter = counter + 1
            t_date, t_company, t_openVal, t_highVal, t_lowVal, t_closeVal = line.split(",")
            s_date = str(t_date)
            company = str(t_company)
            openVal = str(t_openVal)
            highVal = str(t_highVal)
            lowVal = str(t_lowVal)
            closeVal = str(t_closeVal)
        global idVal
        idVal = s_date
        batch.add(insertQuery, (idVal, company, openVal, highVal, lowVal, closeVal))
        if counter == how_many_at_a_time:
            print("Inserting batch of "+str(counter)+" elements.")
            start = time.time()
            self.session.execute(batch)
            end = time.time()
			# Send the response time to monitor server.
            print("Response time : "+str(end - start)+ " seconds.")
            counter = 0
			# After inserting bunch of records, sleep for some time.
            print("Going to sleep.")
            time.sleep(sleep_interval)
            print("Waking up...")
        if counter > 0:
            print("Inserting  last batch of "+str(counter)+" elements.")
            start = time.time()
            self.session.execute(batch)
            end = time.time()
            # Send the response time to monitor server.
            print("Response time : "+str(end - start)+ " seconds.")
        out_file.close()
        print('Data loaded.')
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------
    def query_schema(self, keyspace, tableName, first_N_rows, howLong, sleep_interval):
        maxTime = time.time() + howLong
        print "%-30s\t%-20s\t%-20s\t%-20s\t%-20s\t%-20s\n%s" % ("id", "Company", "OpenValue", "HighValue", "LowValue", "CloseValue", "-------------------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------")
        while time.time() < maxTime:
            start = time.time()
            results = self.session.execute("""SELECT * FROM """+keyspace+"""."""+tableName+""" LIMIT """+str(first_N_rows)+""" ;""")
            end = time.time()
        for row in results:
            print "%-30s\t%-20s\t%-20s\t%-20s\t%-20s\t%-20s" % (row.id, row.company, row.open, row.high, row.low, row.close)
        print("")
        print("Response time : "+str(end - start)+" seconds.")
        print("Going to sleep.")
        time.sleep(sleep_interval)
        print("Waking up...")
        print("Data  retrived :")
        print("")
        print('Schema queried.')
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------