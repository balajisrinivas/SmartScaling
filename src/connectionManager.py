from cassandra.cluster import Cluster
import logging
import yaml
from fileinput import close
from configManager import ConfigManager
from connection import SimpleClient
from time import sleep

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
#        for i in range(0, self.configMgr.getNumConnections() - 1):
        self.clientConnections.append(SimpleClient())
        self.clientConnections[0].connect(self.configMgr.getNodes())
        keyspace = "balajiSpace"
        replication = "3"
        tableName = "expData"
        how_many_at_a_time = 500 # How many records to insert in a batch.
        howLong = 10 # Run for 120 seconds. 
        NUM_REQUESTS = 24
        sleep_interval = 1.0/NUM_REQUESTS # control distribution of a second
        file_seek_pos = 0
        
        #self.clientConnections[0].create_schema(keyspace, replication)
        #self.clientConnections[0].create_column_family(keyspace, tableName)
        #self.clientConnections[0].load_data(keyspace, tableName, how_many_at_a_time, sleep_interval, NUM_REQUESTS, file_seek_pos)
        #self.clientConnections[0].query_schema(keyspace, tableName, how_many_at_a_time, sleep_interval, NUM_REQUESTS, file_seek_pos)
        self.clientConnections[0].getAndWrite(keyspace, tableName, how_many_at_a_time, sleep_interval, NUM_REQUESTS, file_seek_pos)
        self.clientConnections[0].close()

def main():
    logging.basicConfig()
    manager = ConnectionManager()

if __name__ == "__main__":
    main()
