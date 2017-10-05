#!/usr/bin/env python
"""
The participant program for CS5414 three phase commit project.
"""
import os
import signal
import sys
import time
from threading import Thread
from socket import AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR, socket, error

# MASK VARIABLE
TIMEOUT = 0.2
ADDR = 'localhost'

# msg type
VOTEREQ = "VOTEREQ"
COMMIT = "COMMIT"
PRECOMMIT = "PRECOMMIT"
ABORT = "ABORT"
HEARTBEAT = "heartbeat"
ELECT = "URELECT"

# Global Variables
playlist = {}
voteREQIDs = set()
partialCommitIDs = set()
partialPrecommitIDs = set() 
isCoordinator = False
alives = {} 
DTlog = []
votes = {} 
acks = set()
DT_PATH = ""
crashAfterVote = False 
crashBeforeVote = False 
crashAfterAck = False 
crashVoteREQ = False 
crashPartialCommit = False 
crashPartialPreCommit = False
waitForVote = False
waitForVoteReq = False
waitForDecision = False
waitForAck = False

# Threads
listeners = {}
clients = {}
master_thread = None 
heartbeat_thread = None

class MasterListener(Thread):
	def __init__(self, pid, num_servers, port):
		global alives, heartbeat_thread
		Thread.__init__(self)
		self.pid = pid
		self.num_servers = num_servers
		self.port = port
		self.buffer = ""

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
		global crashAfterVote, crashBeforeVote, crashAfterAck, crashPartialCommit, crashPartialPreCommit, crashVoteREQ

		# First client 
		if len(alives) == 0:
			isCoordinator = True
			print "coordinator " + str(self.pid)
			self.master_conn.send("coordinator {:d}\n".format(self.pid))
		while self.connected:
			if '\n' in self.buffer:
				(l, rest) = self.buffer.split("\n", 1)
				self.buffer = rest
				cmd, msgs = l.split(None, 1)
				cmd = cmd.strip()
				if cmd == "get":
					url = playlist.get(msgs.strip(), 'NONE')
					self.master_conn.send("resp {}\n".format(url))
				elif cmd == "crash":
					# if isCoordinator: 
					# 	new_coordinator("")
					self.kill()
					exit()
				# participant crash
				elif cmd == "crashAfterVote" and not isCoordinator:
					afterVote = True 
				elif cmd == "crashBeforeVote" and not isCoordinator:
					beforeVote = True 
				elif cmd == "crashAfterAck" and not isCoordinator:
					afterAck = True 
				
				# For coordinator only
				elif cmd == "add" or cmd == "delete":
					if isCoordinator: 
						self.run_protocol(l)
					else:
						pass
				elif cmd == "crashVoteREQ" and isCoordinator:
					crashVoteREQ = True 
					voteREQIDs = set([int(val) for val in msgs.split() if val])
				elif cmd == "crashPartialPreCommit" and isCoordinator:
					crashPartialPreCommit = True 
					partialPreCommitIDs = set([int(val) for val in msgs.split() if val])
				elif cmd == "crashPartialCommit" and isCoordinator:
					crashPartialCommit = True 
					partialCommitIDs = set([int(val) for val in msgs.split() if val])
				else:
					print "Unknown command {}".format(l)
			else:
				try:
					data = self.master_conn.recv(1024)
					self.buffer += data
				except:
					self.kill()
					break 

	def run_protocol(self, l):
			global votes, alives, playlist
			# Keep track of the set of alive process at the time of VOTEREQ
			aliveset = alives.keys() 
			vote_req(l)
			waitForVote = True
			DTlog.append("Start-3PC")
			signal.signal(signal.SIGALRM, self.timeout_handler)
			signal.alarm(TIMEOUT)
			while waitForVote:
				if (aliveset - set(votes.keys())):
					continue
				else:
					waitForVote = False
					signal.alarm(0) # deregister the alarm
			if False in votes.values():
				DTlog.append("ABORT on command {}".format(msgs[1:]))
				self.master_conn.send("ack ABORT\n")
				abort()
			else:
				# all Yes
				args = l.split()
				cmd, name, url = args[0], args[1], args[2]
				if cmd == "add" and len(url) > self.pid + 5:
					# Coordinator voted no
					abort()
					return
				# precommit and commit 
				pre_commit(l)
				waitForAck = True
				signal.alarm(TIMEOUT)
				while waitForAck:
					if set(alives.keys()) - acks:
						continue
					else:
						waitForAck = False
						signal.alarm(0)
				# After receiving the ACKs
				if cmd == "add": 
					playlist[name] = url
				else:
					# catch the case that delete item doesn't exist
					try: 
						del playlist[name]
					except:
						pass
				DTlog.append("COMMIT on command {}".format(l))
				self.master_conn.send("ack COMMIT\n")
				commit(l)			
			votes = {} 

	def timeout_handler(self):
		if waitForVote:
			DTlog.append("ABORT on command {}".format(msgs[1:]))
			self.master_conn.send("ack ABORT\n")
			abort()
			votes = {}

	def kill(self):
		try:
			self.connected = False
			self.master_conn.close()
			self.socket.close()
		except:
			pass


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
		global crashBeforeVote, waitForDecision,crashAfterAck, crashAfterVote
		self.conn, self.addr = self.sock.accept()
		self.connected = True 
		while True: 
			if "\n" in self.buffer: 
				(l, rest) = self.buffer.split("\n", 1)
				self.buffer = rest 
				msgs = l.split()
				if msgs[0] == HEARTBEAT: 
					alives[self.target_pid] = time.time()
				elif msgs[0] == VOTEREQ:
					COOR_ID = self.target_pid
					cmd = msgs[1]
					if crashBeforeVote:
						crashBeforeVote = False 
						exit()
					if msgs[1] == "add":
						name, url = msgs[2], msgs[3]
						if len(url) > self.pid + 5:
							reply(self.target_pid, "NO {:d}\n".format(self.pid))
							DTlog.append("ABORT on command {}".format(msgs[1:]))
					else:
						#Otherwise alwasy Vote YES
						reply(self.target_pid, "YES {:d}\n".format(self.pid))
						DTlog.append("YES on command {}".format(msgs[1:]))
						waitForDecision = True
					if crashAfterVote: 
						crashAfterVote = False 
						exit()
						break
					# Wait for PRECOMMIT
					if waitForDecision:
						signal.signal(signal.SIGALRM, self.timeout_handler)
						signal.alarm(TIMEOUT)
						while waitForDecision:
							if not waitForDecision:
								signal.alarm(0)
								break
							time.sleep(0.05)
				elif msgs[0] == PRECOMMIT:
					COOR_ID = self.target_pid
					waitForDecision = False
					reply(self.target_pid, "ACK {:d}\n".format(self.pid))
					if crashAfterAck: 
						crashAfterAck = False 
						exit()
						break
					# Wait for COMMIT
					waitForDecision = True
					signal.alarm(TIMEOUT)
					while waitForDecision:
						if not waitForDecision:
							signal.alarm(0)
							break
						time.sleep(0.05)
				elif msgs[0] == COMMIT: 
					COOR_ID = self.target_pid
					waitForDecision = False
					if msgs[1] == 'add':
						name, url = msgs[2], msgs[3]
						playlist[name] = url 
					elif msgs[1] == 'delete':
						try:
							del playlist[msgs[2]]
						except:
							pass
				elif msgs[0] == "NO": 
					votes[self.target_pid] = False
				elif msgs[0] == "YES": 
					votes[self.target_pid] = True
				elif msgs[0] == "ACK":
					acks.add(self.target_id)
				elif msgs[0] == ELECT:
					# Elected as the new coordinator
					isCoordinator = True
					print "coordinator " + str(self.pid)
					self.conn.send("coordinator {:d}\n".format(self.pid))	
					self.run_election_protocol()
			else: 
				try: 
					data = self.conn.recv(1024)
					if data == "": 
						raise ValueError
					self.buffer += data 
				except: 
					self.conn = None 
					self.conn, self.addr = self.sock.accept()

		def timeout_handler(self):
			if waitForDecision:
				run_election(COOR_ID, self.num_servers)
				# need to run election protocol

		def run_election_protocol(self):
			# Send STATE-REQ to all processes

			# Wait for state report

			# If any state report is aborted, coordinate writes and send abort

			# if ALL state report are commit, coordinate commits  

			# If ALL state report were uncertain and the coordinator itself is 
			# also uncertain, write and send abort

			# if SOME process are committable:
			# send pre-commit 
			pass

	def kill(self):
		try:
			self.conn.close()
			self.sock.close()
		except:
			pass


class ServerClient(Thread):
  def __init__(self, pid, target_pid):
  	Thread.__init__(self)
  	self.pid = pid
  	self.target_pid = target_pid 
  	self.port = 29999 - target_pid * 100 - pid
  	self.sock = None 

  def run(self):
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


def run_election(pid, num_servers): 
	global clients, alives 
	next = (pid + 1) % num_servers
	while next != pid: 
		if next in alives: 
			clients[next].send(ELECT)
			break
		next = (next + 1) % max_num_servers 


def broadcast(msg):
	global clients, alives
	for pid, client in clients.items():
		if pid in alives:
			try:
				client.send(msg)
			except:
				pass


def commit(msg):
	global crashPartialCommit
	if isCoordinator:
		message = "{} {}".format(COMMIT, msg)
		if crashPartialCommit:
			crashPartialCommit = False
			for pid in partialCommitIDs:
				reply(pid, message)
			exit()
		else:
			broadcast(message)


def pre_commit(msg):
	global crashPartialPreCommit
	if isCoordinator:
		message = "{} {}".format(PRECOMMIT, msg)
		if crashPartialPreCommit:
			crashPartialPreCommit = False
			for pid in partialPrecommitIDs:
				reply(pid, message)
			exit()
		else:
			broadcast(message)


def vote_req(msg): 
	global crashVoteREQ
	if isCoordinator: 
		message = "{} {}".format(VOTEREQ, msg)
		if crashVoteREQ:
			crashVoteREQ = False
			for pid in voteREQIDs:
				reply(pid, message)
			exit()
		else:
			broadcast(message)


def abort(command):
	global votes, alives, clients
	DTlog.append("ABORT on command {}".format(l))
	if isCoordinator:
		# send abort to those who voted 
		for key in votes: 
			if key in alives and votes[key]: 
				try: 
					clients[key].send(ABORT); 
				except: 
					pass


def reply(target_pid, msg):
	global clients
	clients[target_pid].send(msg)


def write_DTlog():
	global DTlog, DT_PATH
	with open(DT_PATH, 'wt') as file:
		for line in DTlog:
			file.write(line)


def load_DTlog():
	global DTlog, DT_PATH
	try:
		with open(DT_PATH, 'rt') as file:
			DTlog = [line for line in file if line]
	except:
		pass

# Master -> commands to coordinator or ask the server to crash
# n listeners and n clients:
def main(pid, num_servers, port):
	global master_thread, DT_PATH
	DT_PATH = "{:d}_DTlog.txt".format(pid)
	load_DTlog()
	master_thread = MasterListener(pid, num_servers, port)
	master_thread.start()
	# sys.stdout.write("Start the master thread \n")
	# sys.stdout.flush()


# Call this function to exit the program
def exit():
	for key in listeners:
		listeners[key].kill()
	for key1 in clients:
		clients[key1].kill() 
	master_thread.kill()
	heartbeat_thread.kill()
	write_DTlog()
	os._exit(0)

if __name__ == '__main__':
	args = sys.argv
	if len(args) != 4:
		print "Need three arguments!"
		os._exit(0)
	try:
		main(int(args[1]), int(args[2]), int(args[3]))
	except KeyboardInterrupt: 
		os._exit(0)