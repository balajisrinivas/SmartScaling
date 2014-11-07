import yaml
from fileinput import close


class ConfigManager:
    def __init__(self):
        self.config_file = 'appconfig.yaml'
        self.nodes = []
        self.num_connections = 1
        self.readConfig()
        
    def readConfig(self):
        fs = file(self.config_file, 'r')
        
        "load yaml"
        tokenList = yaml.load(fs)
        
        "read config params"
        self.nodes = tokenList['nodes']
        self.num_connections = tokenList['num_connections']
	print self.nodes
        
        fs.close()
        
    def getNodes(self):
        return self.nodes
    
    def getNumConnections(self):
        return self.num_connections
    
