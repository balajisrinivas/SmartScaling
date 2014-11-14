from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.query import BatchStatement

import time
import logging

log = logging.getLogger()
log.setLevel('INFO')

class SimpleClient:
    session = None

    def connect(self, nodes):
        cluster = Cluster(nodes)
        metadata = cluster.metadata
        self.session = cluster.connect()
        print('Connected to cluster: ' + metadata.cluster_name)

        for host in metadata.all_hosts():
            print('Datacenter: %s; Host: %s; Rack: %s',
                host.datacenter, host.address, host.rack)
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------
    def close(self):

        self.session.cluster.shutdown()
        self.session.shutdown()
        log.info('Connection closed.')
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------
    def create_schema(self, keyspace, replication):

	create_keyspace = """CREATE KEYSPACE """+keyspace+""" WITH replication = 
				{'class':'SimpleStrategy', 'replication_factor':"""+replication+"""};"""	
	self.session.execute(create_keyspace)
        print('Keyspace created.')
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------
    def create_column_family(self, keyspace, tableName):

	create_column_family = """CREATE TABLE """+keyspace+"""."""+tableName+""" (id text PRIMARY KEY, company text, open text, high text, low text, close text);"""	
	self.session.execute(create_column_family)
	print("Column family created.")
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------
    def load_data(self, keyspace, tableName, number):

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

		if counter == number:
			print("Inserting batch of "+str(counter)+" elements.")

			start = time.time()
			self.session.execute(batch)
			end = time.time()

			# Send the response time to monitor server.
			print("Response time : "+str(end - start)+ " seconds.")
			counter = 0


	if counter > 0 :
		print("Inserting  last batch of "+str(counter)+" elements.")
		start = time.time()
		self.session.execute(batch)
		end = time.time()

		# Send the response time to monitor server.
		print("Response time : "+str(end - start)+ " seconds.")

	out_file.close()
        print('Data loaded.')
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------
    def query_schema(self, keyspace, tableName):

	start = time.time()
        results = self.session.execute("""
	    SELECT * FROM """+keyspace+"""."""+tableName+""" ;""")
	end = time.time()
        print("Data  retrived :")
	print("")

	print "%-30s\t%-20s\t%-20s\t%-20s\t%-20s\t%-20s\n%s" % ("id", "Company", "OpenValue", "HighValue", "LowValue", "CloseValue", "-------------------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------")

        for row in results:
            print "%-30s\t%-20s\t%-20s\t%-20s\t%-20s\t%-20s" % (row.id, row.company, row.open, row.high, row.low, row.close)
	
	print("")
	print("Response time : "+str(end - start)+" seconds.")
        print('Schema queried.')
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------

