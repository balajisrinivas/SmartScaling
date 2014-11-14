from cassandra.cluster import Cluster
import logging
import yaml
from fileinput import close
from configManager import ConfigManager
from connection import SimpleClient

log = logging.getLogger()
log.setLevel('INFO')

config_file = 'appconfig.yaml'

class ConnectionManager:
    def __init__(self):
        self.configMgr = None
        self.clientConnections = []
        self.initialize()
        
    def initialize(self):
        self.configMgr = ConfigManager()
        for i in range(0, self.configMgr.getNumConnections() - 1):
            self.clientConnections.append(SimpleClient())
	    
        self.clientConnections[0].connect(self.configMgr.getNodes())
	keyspace = "finSpa"
	replication = "2"
	tableName = "finData"
	how_many_at_a_time = 100 # How many records to insert in a batch.
	howLong = 120 # Run for 120 seconds.
	sleep_interval = 3 # Sleep for 3 seconds.
	first_N_rows = 500 # how many rows to fetch at a time.

	#self.clientConnections[0].create_schema(keyspace, replication)
	#self.clientConnections[0].create_column_family(keyspace, tableName)
	self.clientConnections[0].load_data(keyspace, tableName, how_many_at_a_time, sleep_interval)
	self.clientConnections[0].query_schema(keyspace, tableName, first_N_rows, howLong, sleep_interval)
	self.clientConnections[0].close()

def main():
    logging.basicConfig()
    manager = ConnectionManager()

if __name__ == "__main__":
    main()
