import sys
import socket

class MonitoringClient(object):
    
    def __init__(self):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #self.hostname = "172.24.26.37"
        self.hostname = "127.0.0.1"
        #self.hostname = "129.49.206.158"
        self.port = 13567
        self.s.connect((self.hostname, self.port))
        
    def __del__(self):
        self.s.close()

    def sendMessage(self, message):
        self.s.send(message.encode('ascii'))
    
    
def main():
    pass
    '''MonitoringClient mc = new MonitoringClient()
    mc.sendMessage("Hello")'''