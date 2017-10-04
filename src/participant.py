#!/usr/bin/env python
"""
The participant program for CS5414 three phase commit project.
"""
import os
import sys
import time
from socket import AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR, socket, error
from threading import Thread


TIMEOUT = 0.01
BASE_PORT = 20000
ADDR = 'localhost'
playlist = {}
listeners = {}
isCoordinator = False
clients = {}
alives = {} 
DTlog = []
self_pid = -1 
votes = {} 
max_num_servers = -1 
master_thread = None 
crashAfterVote = False 
crashBeforeVote = False 
crashAfterAck = False 
crashVoteREQ = False 
crashPartialCommit = False 
crashPartialPreCommit = False 
voteREQList = [] 
partialCommitList = []
partialPrecommitList = [] 

# msg type
VOTEREQ = "VOTEREQ"
COMMIT = "COMMIT"
PRECOMMIT = "PRECOMMIT"
ABORT = "ABORT"
HEARTBEAT = "heartbeat"

class Heartbeat(Thread): 
	def __init__(self, index): 
		Thread.__init__(self)
		self.index = index

	def run(self): 
		global alives 
		while True: 
			new_alives = {} 
			now = time.time()
			for key in alives: 
				if now - alives[key] <= 0.2: 
					new_alives[key] = alives[key]
			alives = new_alives
			time.sleep(0.2)

class MasterListener(Thread):
	def __init__(self, pid, port):
		global alives
		Thread.__init__(self)
		self.pid = pid
		self.num_servers = max_num_servers
		self.port = port
		self.buffer = ""

		base_port = BASE_PORT + pid*self.num_servers*2

		for i in range(self.num_servers):
			if i != pid:
				listeners[i] = ServerListener(pid, i)
				listeners[i].start()

		for i in range(self.num_servers): 
			if (i != pid): 
				clients[i] = ServerClient(pid, i) 
				clients[i].start()

		heartbeat_thread = Heartbeat(pid)
		heartbeat_thread.start()

		self.socket = socket(AF_INET, SOCK_STREAM)
		self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.socket.bind((ADDR, self.port))
		self.socket.listen(1)
		self.master_conn, self.master_addr = self.socket.accept()
		self.connected = True

	def run(self):
		global alives, isCoordinator, DTlog, clients, votes, playlist
		global crashAfterVote, crashBeforeVote, crashAfterAck
		# First participant
		if len(alives) == 0:
			# sys.stdout.write('no alive processes {:d}\n'.format(self.pid))
			# sys.stdout.flush()
			isCoordinator = True
			print "coordinator " + str(self.pid)
			self.master_conn.send("coordinator {:d}\n".format(self.pid))
		# else:
		# 	sys.stdout.write(alives)
		# 	sys.stdout.flush()
		while self.connected:
			if '\n' in self.buffer:
				(l, rest) = self.buffer.split("\n", 1)
				self.buffer = rest
				msgs = l.split(None, 1)
				cmd = msgs[0].strip()
				if cmd == "get":
					url = playlist.get(msgs[1].strip(), 'NONE')
					self.master_conn.send("resp {}\n".format(url))
				elif cmd == "crash":
					if isCoordinator: 
						new_coordinator("")
					self.kill()
					exit()
				elif cmd == "crashAfterVote":
					if not isCoordinator: 
						global afterVote
						afterVote = True 
				elif cmd == "crashBeforeVote":
					if not isCoordinator: 
						global beforeVote 
						beforeVote = True 
				elif cmd == "crashAfterAck":
					if not isCoordinator: 
						global afterAck 
						afterAck = True 
				# For coordinator only
				elif cmd == "add" or cmd == "delete":
					if isCoordinator: 
						vote_req(l)
						DTlog.append("Start-3PC")
						abort_transaction = True 
						time.sleep(TIMEOUT)
						if (len(votes) != len(alives)): 
							DTlog.append("ABORT on command {}".format(l))
							abort()
							votes = {} 
							# didn't get all votes, timeout 
						else: 
							allyes = True 
							for key in votes: 
								if not votes[key]: 
									allyes = False 
									break 
							if allyes: 
								# precommit and commit 
								votes = {} 
								pre_commit(l)
								time.sleep(TIMEOUT)
								arguments = l.split()
								if cmd == "add": 
									playlist[arguments[1]] = arguments[2]
								else: 
									del playlist[arguments[1]]
								DTlog.append("COMMIT on command {}".format(l))
								commit(l)
								abort_transaction = False
								pass 
							else: 
								DTlog.append("ABORT on command {}".format(msgs[1:]))
								abort()
								votes = {} 
						# sys.stdout.write("Start-3PC")
						# sys.stdout.flush()
						if abort_transaction: 
							self.master_conn.send("ack ABORT\n")
						else: 
							self.master_conn.send("ack COMMIT\n")
				elif cmd == "crashVoteREQ":
					crashVoteREQ = True 
					voteREQList = msgs[1].split()
				elif cmd == "crashPartialPreCommit":
					crashPartialPreCommit = True 
					partialPrecommitList = msgs[1].split()
				elif cmd == "crashPartialCommit":
					crashPartialCommit = True 
					partialCommitList = msgs[1].split()
				else:
					sys.stdout.write("Unknown command {}".format(l))
					sys.stdout.flush()
			else:
				try:
					data = self.master_conn.recv(1024)
					self.buffer += data
				except:
					self.kill()
					break 

	def kill(self):
		try:
			self.connected = False
			self.master_conn.close()
			# self.socket.close()
		except:
			pass

def broadcast(msg):
	global clients, alives
	for pid, client in clients.items():
		if pid in alives:
			try:
				client.send(msg)
			except:
				pass

def new_coordinator(l): 
	global max_num_servers, clients, alives 
	if isCoordinator: 
		next = (self_pid + 1) % max_num_servers
		while next != self_pid: 
			if next in alives: 
				clients[next].send("new " + l) 
				break; 
			next = (next + 1) % max_num_servers 

def commit(l):
	if isCoordinator:
		broadcast(COMMIT + " " + l)

def pre_commit(l):
	if isCoordinator:
		broadcast(PRECOMMIT + " " + l)

def vote_req(l): 
	if isCoordinator: 
		broadcast(VOTEREQ + " " + l)

def abort():
	global votes, alives, clients
	if isCoordinator:
		# send abort to those who voted 
		for key in votes: 
			if votes[key]: 
				if key in alives: 
					try: 
						clients[key].send(ABORT); 
					except: 
						pass 

def reply(target_pid, msg):
	global clients
	clients[target_pid].send(msg)

class ServerListener(Thread): 
	def __init__(self, pid, target_pid): 
		global ADDR 
		Thread.__init__(self)
		self.pid = pid
		self.target_pid = target_pid 
		self.sock = socket(AF_INET, SOCK_STREAM)
		self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.port = 29999 - pid * 100 - target_pid 
		self.sock.bind((ADDR, self.port))
		self.sock.listen(1)
		self.buffer = ''

	def run(self): 
		self.conn, self.addr = self.sock.accept()
		self.connected = True 
		global alives, DTlog, COOR_ID, votes, crashAfterAck, crashAfterVote, crashBeforeVote
		while True: 
			if "\n" in self.buffer: 
				(l, rest) = self.buffer.split("\n", 1)
				self.buffer = rest 
				msgs = l.split()
				if (msgs[0] == HEARTBEAT): 
					alives[self.target_pid] = time.time()
				elif msgs[0] == VOTEREQ:
					COOR_ID = self.target_pid
					if crashBeforeVote: 
						master_thread.kill()
						crashBeforeVote = False 
						break; 
					if msgs[1] == 'add':
						name, url = msgs[2], msgs[3]
						if len(url) > self.pid + 5:
							#Vote No
							reply(self.target_pid, "NO {:d}\n".format(self.pid))
							DTlog.append("ABORT on command {}".format(msgs[1:]))
						else:
							#Vote YES
							reply(self.target_pid, "YES {:d}\n".format(self.pid))
					elif msgs[1] == 'delete':
						name = msgs[2]
						if len(url) > self.pid + 5:
							#Vote No
							reply(self.target_pid, "NO {:d}\n".format(self.pid))
							DTlog.append("ABORT on command {}".format(msgs[1:]))
						else:
							#Vote YES
							reply(self.target_pid, "YES {:d}\n".format(self.pid))
					if crashAfterVote: 
						master_thread.kill()
						crashAfterVote = False 
						break; 
				elif msgs[0] == "NO": 
					votes[self.target_pid] = False 
				elif msgs[0] == "YES": 
					votes[self.target_pid] = True 
				elif msgs[0] == PRECOMMIT: 
					COOR_ID = self.target_pid
					reply(self.target_pid, "ACK {:d}\n".format(self.pid))
					if crashAfterAck: 
						master_thread.kill()
						crashAfterAck = False 
						break; 
				elif msgs[0] == COMMIT: 
					COOR_ID = self.target_pid
					if msgs[1] == 'add':
						name, url = msgs[2], msgs[3]
						playlist[name] = url 
					elif msgs[1] == 'delete':
						name = msgs[2]
						del playlist[name]
				elif msgs[0] == "ACK": 
					pass 
				elif msgs[0] == "new": 
					isCoordinator = True
					print "coordinator " + str(self.pid)
					self.conn.send("coordinator {:d}\n".format(self.pid))

			else: 
				try: 
					data = self.conn.recv(1024)
					if data == "": 
						raise ValueError
					self.buffer += data 
				except: 
					self.kill()
					self.conn = None 
					self.conn, self.addr = self.sock.accept()	

	def kill(self):
		try:
			self.connected = False
			self.conn.close()
			# self.sock.close()
		except:
			pass

class ServerClient(Thread):
  def __init__(self, pid, target_pid):
  	global ADDR 
  	Thread.__init__(self)
  	self.pid = pid
  	self.target_pid = target_pid 
  	self.port = 29999 - target_pid * 100 - pid
  	self.sock = None 

  def run(self):
  	global ADDR 
  	while True: 
  		self.send(HEARTBEAT)
  		time.sleep(0.05) 

  def send(self, msg): 
  	if not msg.endswith("\n"): 
  		msg = msg + "\n"
  	try: 
  		self.sock.send(msg)
  		self.connected = True 
  	except: 
  		try: 
  			self.sock = None 
  			s = socket(AF_INET, SOCK_STREAM)
  			s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
  			s.connect((ADDR, self.port))
  			self.sock = s 
  			self.sock.send(msg)
  			self.connected = True 
  		except: 
  			self.connected = False 
  			pass 

  def kill(self):
		try:
			self.connected = False
			self.sock.close()
		except:
			pass

def exit():
	for key in listeners:
		listeners[key].kill()
	for key1 in clients:
		clients[key1].kill() 
	os._exit(0)


# Master -> commands to coordinator or ask the server to crash
# n listeners and n clients:
# From 20000 up, each process allocates 2n ports, the first n as listeners.
def main(pid, num_servers, port):
	global self_pid, max_num_servers, master_thread
	self_pid = pid
	max_num_servers = num_servers
	master_thread = MasterListener(pid, port)
	master_thread.start()
	# sys.stdout.write("Start the master thread \n")
	# sys.stdout.flush()

if __name__ == '__main__':
	# print ("main")
	args = sys.argv
	if len(args) != 4:
		# sys.stdout.write("Need three arguments!")
		# sys.stdout.flush()
		os._exit(0)
	try:
		# sys.stdout.write("start the program with {} {} {}\n".format(args[0], args[1], args[2]))
		# sys.stdout.flush()
		main(int(args[1]), int(args[2]), int(args[3]))
	except KeyboardInterrupt: 
		os._exit(0)