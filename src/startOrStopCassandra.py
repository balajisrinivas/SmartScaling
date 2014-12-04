from fabric.api  import *
import yaml
import random

class StartOrStop:
	def __init__(self):
		config_file = 'appconfig.yaml'
		fs = file(config_file, 'r')        
		tokenList = yaml.load(fs)
		self.startedServers = tokenList['nodes']
		self.stoppedServers = []
		
		print(self.startedServers)

	def startCassandra(self):
		if len(self.stoppedServers) == 0:
			print("All servers in the cluster are running already.")
			return
		random_host = random.choice(self.stoppedServers)
		env.host_string = random_host
		env.user = 'ubuntu'
		env.key_filename = '/home/mujay/Desktop/EnergyEfficientComuting/project/bgms.pem'

		with hide('running'):
			run('nohup ./apache-cassandra-2.1.0/bin/cassandra')
		print("Started Cassandra in :"+str(random_host))
		self.startedServers.append(random_host)
		self.stoppedServers.remove(random_host)

	def stopCassandra(self):
		if len(self.startedServers) == 0:
			print("No server is running currently.")
			return
		random_host = random.choice(self.startedServers)
		env.host_string = random_host
		env.user = 'ubuntu'
		env.key_filename = '/home/mujay/Desktop/EnergyEfficientComuting/project/bgms.pem'
		with settings(warn_only=True):
			run('./apache-cassandra-2.1.0/bin/stop-server')

		print("Stopped Cassandra in :"+str(random_host))
		self.stoppedServers.append(random_host)
		self.startedServers.remove(random_host)


def main():
	obj = StartOrStop()
	while 1:
		choice = int(input("Enter the choice.\n 1. Start cassandra\n 2. Stop cassandra.\n"))

		if choice == 1:
			obj.startCassandra()
		elif choice == 2:
			obj.stopCassandra()
		else:
			break

main()

