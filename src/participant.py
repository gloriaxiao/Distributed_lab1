#!/usr/bin/env python
"""
The participant program for CS5414 three phase commit project.
"""
import os
import sys
import time
from threading import Thread
from socket import AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR, socket, error

# MASK VARIABLE
TIMEOUT = 0.2
SLEEP = 0.05
ADDR = 'localhost'

# msg type
VOTEREQ = "VOTEREQ"
STATEREQ = "STATEREQ"
COMMIT = "COMMIT"
PRECOMMIT = "PRECOMMIT"
COMMITTABLE = "COMMITTABLE"
UNCERTAIN = "UNCERTAIN"
ABORT = "ABORT"
HEARTBEAT = "heartbeat"
ELECT = "URELECT"

# Global Variables
DTlog = []
playlist = {}
DT_PATH = ""

# ID sets
voteREQIDs = set()
partialCommitIDs = set()
partialPrecommitIDs = set()

# vote/state requests
vote_reqs = set()
state_reqs = set()
votes = {}
states = {}

# Coordinate states 
isCoordinator = False
COOR_ID = -1
died_coor = set()

# Server state
alives = {} 
self_pid = -1
max_num_servers = -1
localstate = ABORT

# 3PC instances
in3PC = False
num_3PC = 0

# container class variables
wait = None
countDown = None
crash = None

# Threads
listeners = {}
clients = {}
master_thread = None 
heartbeat_thread = None
timeout_thread = None

class CountDown:
	def __init__(self):
		self.start = False

	def begin(self):
		self.start = True

	def stop(self):
		self.start = False

class Wait:
	def __init__(self):
		self.waitForVote = False
		self.waitForVoteReq = False
		self.waitForState = False
		self.waitForStateReq = False
		self.waitForStateResp = False
		self.waitForCommit = False
		self.waitForPreCommit = False
		# self.waitForACK = False
		self.waiting_cmd = ""

	def finished_waiting(self):
		wait = self.waitForVote + self.waitForVoteReq + self.waitForState + \
		self.waitForStateReq + self.waitForStateResp + self.waitForCommit + \
		self.waitForPreCommit
		return (not wait)


class Crash:

	def __init__(self):
		self.crashAfterVote = False 
		self.crashBeforeVote = False 
		self.crashAfterAck = False 
		self.crashVoteREQ = False 
		self.crashPartialCommit = False 
		self.crashPartialPreCommit = False

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
		heartbeat_thread.setDaemon(True)
		heartbeat_thread.start()
		self.socket = socket(AF_INET, SOCK_STREAM)
		self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.socket.bind((ADDR, self.port))
		self.socket.listen(1)
		self.master_conn, self.master_addr = self.socket.accept()
		self.connected = True

	def run(self):
		global alives, isCoordinator, DTlog, clients, votes, playlist, crash, COOR_ID
		global partialCommitIDs, partialPreCommitIDs, voteREQIDs
		# First client 
		if len(alives) == 0:
			isCoordinator = True
			COOR_ID = self.pid
			print "coordinator {:d}".format(self.pid)
			self.master_conn.send("coordinator {:d}\n".format(self.pid))
		while self.connected:
			if '\n' in self.buffer:
				(l, rest) = self.buffer.split("\n", 1)
				self.buffer = rest
				cmd = l.split()[0]
				print l
				if cmd == "get":
					_, msgs = l.split(None, 1)
					url = playlist.get(msgs.strip(), 'NONE')
					self.master_conn.send("resp {}\n".format(url))
				# crashes
				elif cmd == "crashAfterVote":
					crash.crashAfterVote = True
				elif cmd == "crashBeforeVote":
					crash.crashBeforeVote = True
				elif cmd == "crashAfterAck":
					crash.crashAfterAck = True 
				elif cmd == "crash":
					print "handle crash"
					exit()
					break
				elif cmd == "crashVoteREQ" and isCoordinator:
					crash.crashVoteREQ = True
					if l == cmd:
						# nothing to send VOTEREQ, immediately crash
						voteREQIDs = set()
					else:
						_, msgs = l.split(None, 1)
						voteREQIDs = set([int(val) for val in msgs.split() if val])
				elif cmd == "crashPartialPreCommit" and isCoordinator:
					crash.crashPartialPreCommit = True 
					if l == cmd:
						partialPreCommitIDs = set()
					else:
						_, msgs = l.split(None, 1)
						partialPreCommitIDs = set([int(val) for val in msgs.split() if val])
						print partialPrecommitIDs
				elif cmd == "crashPartialCommit" and isCoordinator:
					crash.crashPartialCommit = True 
					if l == cmd:
						partialCommitIDs = set()
					else:
						_, msgs = l.split(None, 1)
						partialCommitIDs = set([int(val) for val in msgs.split()])
				# For coordinator only
				elif cmd == "add" or cmd == "delete":
					print "run 3pc protocol"
					print l
					self.start_VoteREQ(l)
				else:
					print "Unknown command {}".format(l)
			else:
				try:
					data = self.master_conn.recv(1024)
					self.buffer += data
				except:
					self.kill()
					break 

	def start_VoteREQ(self, l):
		global votes, vote_reqs, alives, wait, in3PC, num_3PC
		# Keep track of the set of alive process at the time of VOTEREQ
		in3PC = True
		num_3PC += 1
		vote_req(l)
		wait.waitForVote = True
		wait.waiting_cmd = l
		countDown.begin()
		# print "Initializing vote requests"

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
		global countDown, wait, crash, DTlog, died_coor, isCoordinator
		global localstate, COOR_ID, alives, playlist, in3PC, num_3PC
		self.conn, self.addr = self.sock.accept()
		self.connected = True 
		wait.waitForVoteReq = True
		while True: 
			if "\n" in self.buffer: 
				(l, rest) = self.buffer.split("\n", 1)
				self.buffer = rest
				msgs = l.split()
				if msgs[0] == HEARTBEAT: 
					alives[self.target_pid] = time.time()
					continue
				elif msgs[0] == STATEREQ:
					print "{:d} receives state req from {:d}".format(self.pid, self.target_pid)
					if COOR_ID != self.target_pid:
						#Didn't detect the failure of Coordinator
						if self.target_pid not in died_coor:
							remove_zombies(self.target_pid)
							COOR_ID = self.target_pid
						else:
							# ignore request from zombie coordinator
							continue
					wait.waitForStateReq = False
					_, command = DTlog[-1].split(None, 1)
					reply(self.target_pid, "STATE {} {}".format(localstate, command))
					wait.waitForStateResp = True
					countDown.begin()
				elif msgs[0] == VOTEREQ:
					wait.waitForVoteReq = False
					_, info = l.split(None, 1)
					in3PC = True
					num_3PC += 1
					cmd = msgs[1]
					if crash.crashBeforeVote:
						crash.crashBeforeVote = False 
						# Decide to ABORT
						DTlog.append("ABORT {}\n".format(info))
						exit()
						break
					vote_yes = True
					if cmd == "add":
						name, url = msgs[2], msgs[3]
						if len(url) > self.pid + 5:
							DTlog.append("ABORT {}\n".format(info))
							print "{:d} respond No on {}".format(self.pid, info)
							reply(self.target_pid, "NO {:d}\n".format(self.pid))
							vote_yes = False
							localstate = ABORT
							in3PC = False
					if vote_yes:
						#Vote YES
						DTlog.append("YES {}\n".format(info))
						print "{:d} respond Yes on {}".format(self.pid, info)
						reply(self.target_pid, "YES {:d}\n".format(self.pid))
						wait.waitForPreCommit = True
						countDown.begin()
						wait.waiting_cmd = info
						localstate = UNCERTAIN
					if crash.crashAfterVote: 
						crash.crashAfterVote = False 
						countDown.stop()
						exit()
						break
				elif msgs[0] == PRECOMMIT:
					print "{:d} get precommit".format(self.pid)
					wait.waitForPreCommit = False
					_, info = l.split(None, 1)
					localstate = COMMITTABLE
					if crash.crashAfterAck: 
						crash.crashAfterAck = False 
						exit()
						break
					print "{:d} replied ACK".format(self.pid)
					reply(self.target_pid, "ACK {:d}\n".format(self.pid))
					wait.waitForCommit = True
					wait.waiting_cmd = info
					countDown.begin()
					time.sleep(SLEEP)
				elif msgs[0] == COMMIT:
					print "{:d} get commit".format(self.pid)
					wait.waitForCommit = False
					_, info = l.split(None, 1)
					DTlog.append(l)
					localstate = COMMIT
					in3PC = False
					if msgs[1] == 'add':
						name, url = msgs[2], msgs[3]
						playlist[name] = url 
					elif msgs[1] == 'delete':
						try:
							del playlist[msgs[2]]
						except:
							pass
				elif msgs[0] == "STATERESP":
					wait.waitForStateResp = False
					_, info = l.split(None, 1)
					cmd, message = info.split(None, 1)
					statelog = info + '\n'
					if (cmd == ABORT or COMMIT):
						if statelog not in DTlog:
							DTlog.append(statelog)
						continue
					reply(self.target_pid, "ACK {:d}\n".format(self.pid))
					wait.waitForCommit = True
					wait.waiting_cmd = message
					countDown.begin()
				elif msgs[0] == "NO":
					votes[self.target_pid] = False
				elif msgs[0] == "YES": 
					votes[self.target_pid] = True
				elif msgs[0] == "STATE":
					_, info = l.split(None, 1)
					# state response
					print "Receive state {} from {:d}".format(info, self.target_pid)
					states[self.target_pid] = info
				elif msgs[0] == ELECT:
					# Elected as the new coordinator
					isCoordinator = True
					COOR_ID = self.pid
					print "receive Election " + str(self.pid)
					start_STATEREQ()
			else:
				try: 
					data = self.conn.recv(1024)
					if data == "": 
						raise ValueError
					self.buffer += data 
				except Exception as e:
					self.conn.close()
					self.conn = None 
					self.conn, self.addr = self.sock.accept()

	def kill(self):
		try:
			self.sock.close()
			self.conn.close()
		except:
			pass


class ServerClient(Thread):
  def __init__(self, pid, target_pid):
  	global max_num_servers
  	Thread.__init__(self)
  	self.pid = pid
  	self.target_pid = target_pid 
  	self.target_port = 29999 - target_pid * 100 - pid
  	self.port = 29999 - 100 * pid - max_num_servers - target_pid
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
  	except Exception as e:
  		if self.sock:
  			self.sock.close()
  			self.sock = None
  		try:
  			new_socket = socket(AF_INET, SOCK_STREAM)
			new_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
			new_socket.bind((ADDR, self.port))
			new_socket.connect((ADDR, self.target_port))
			self.sock = new_socket
			alives[self.target_pid] = time.time()
		except:
			time.sleep(SLEEP)

  def kill(self):
		try:
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


def remove_zombies(new_coor_id):
	global alives, COOR_ID, max_num_servers
	if new_coor_id > COOR_ID:
		for i in range(COOR_ID, new_coor_id):
			if i in alives:
				del alives[i]
	else:
		for i in range(COOR_ID, max_num_servers):
			if i in alives:
				del alives[i]
		for i in range(0, new_coor_id):
			if i in alives:
				del alives[i]


def run_3PC(l):
	global crash, votes, master_thread, DTlog, playlist
	global ack_reqs, wait, localstate, alives, in3PC, ABORT
	if False in votes.values():
		DTlog.append("ABORT {}\n".format(l))
		localstate = ABORT
		master_thread.master_conn.send("ack ABORT\n")
		votes = {}
		abort(l)
		return
	# all Yes
	votes = {}
	args = l.split()
	cmd, name = args[0], args[1]
	if cmd == "add" and len(args[2]) > self_pid + 5:
		# Coordinator voted no
		# print "abort on add"
		abort(l)
		return
	# precommit and commit 
	DTlog.append("YES {}\n".format(l))
	pre_commit(l)
	if crash.crashAfterAck:
		# Coordinator can also get crash after ACK since it implicitly sends itself
		# an ACK
		crash.crashAfterAck = False
		exit()
		return
	args = l.split()
	cmd, name = args[0], args[1]
	# After receiving the ACKs
	if cmd == "add":
		print "add {} {}".format(name, args[2])
		playlist[name] = args[2]
	else:
		# catch the case that delete item doesn't exist
		try: 
			# print "Move on with deletion"
			del playlist[name]
		except:
			pass
	master_thread.master_conn.send("ack COMMIT\n")
	commit(l)
	in3PC = False


def timeout():
	global wait, countDown, votes, vote_reqs, self_pid, master_thread, DTlog
	starttime = 0
	while True:
		if countDown.start:
			if not starttime:
				# set the start time of the countdown process
				starttime = time.time()
				# print "{:d} set start time to {:f}".format(self_pid, starttime)
			elif (time.time() - starttime) > TIMEOUT:
				# Timeout
				countDown.stop()
				starttime = 0 
				if wait.waitForState:
					print "Timeout waiting for State Info"
					wait.waitForState = False
					run_3PC_termination()
				elif wait.waitForStateReq:
					print "Timeout waiting for State Request"
					wait.waitForStateReq = False
					run_election()
				elif wait.waitForStateResp:
					print "Timeout waiting for State Response from Coor"
					wait.waitForStateResp = False
					run_election()
				elif wait.waitForVote:
					print "Timeout waiting for votes"
					wait.waitForVote = False
					master_thread.master_conn.send("ack ABORT\n")
					abort(wait.waiting_cmd)
					wait.waiting_cmd = ""
					votes = {}
				elif wait.waitForVoteReq:
					print "Timeout waiting for vote requests"
					DTlog.append("ABORT TIMEOUT VOTEREQ\n")
					localstate = ABORT
					wait.waitForVoteReq = False
					return
				elif wait.waitForPreCommit:
					print "Timeout waiting for precommit"
					DTlog.append("ABORT TIMEOUT PRECOMMIT\n")
					wait.waitForPreCommit = False
					localstate = UNCERTAIN
					run_election()
				elif wait.waitForCommit:
					print "Timeout waiting for commit"
					DTlog.append("COMMIT TIMEOUT COMMIT\n")
					wait.waitForCommit = False
					localstate = COMMITTABLE
					run_election()
			elif wait.waitForVote:
				if not (vote_reqs - set(votes.keys())):
					print "get all votes"
					cmd = wait.waiting_cmd
					wait.waitForVote = False
					wait.waiting_cmd = ""
					countDown.stop()
					starttime = 0
					run_3PC(cmd)
			elif wait.waitForState:
				if not (state_reqs - set(states.keys())):
					print "get all states"
					wait.waitForState = False
					wait.waiting_cmd = ""
					countDown.stop()
					starttime = 0
					run_3PC_termination()
			elif wait.finished_waiting():
				countDown.stop()
				starttime = 0	
			else:
				time.sleep(SLEEP)
		else:
			time.sleep(SLEEP)


def run_election(): 
	global clients, alives, COOR_ID, max_num_servers, wait, died_coor, self_pid
	global ELECT
	new_coor = (COOR_ID + 1) % max_num_servers
	died_coor.add(COOR_ID)
	if COOR_ID in alives:
		del alives[COOR_ID]
	while new_coor != COOR_ID: 
		if new_coor in alives: 
			clients[new_coor].send(ELECT)
			if new_coor in died_coor:
				died_coor.remove(new_coor)
			COOR_ID = new_coor
			print "New Coordinator {:d}".format(COOR_ID)
			wait.waitForStateReq = True
			break
		if new_coor == self_pid:
			isCoordinator = True
			COOR_ID = self_pid
			print "{:d} Elected as Coordinator".format(self_pid)
			start_STATEREQ()
			break
		new_coor = (new_coor + 1) % max_num_servers 


def start_STATEREQ():
	global master_thread, state_reqs, self_pid, STATEREQ, wait, countDown
	master_thread.master_conn.send("coordinator {:d}\n".format(self_pid))	
	# Send STATE-REQ to all processes
	print "send state requests"
	state_reqs = set(alives.keys())
	broadcast(STATEREQ)
	wait.waitForState = True
	countDown.begin()


def run_3PC_termination():	
	# If any state report is aborted, coordinate writes and send abort
	global DTlog, localstate, states
	uncertains = set()
	statelog = DTlog[-1]
	coor_state, val = statelog.split(None, 1)
	if coor_state == ABORT:
		print 'Coordinator decides ABORT'
		abort(val, state_resp=True)
		return
	uncertain_cmd = ""
	uncertain_pids = set()
	has_committable = False
	for pid, info in states.items():
		state, msg = info.split(None, 1)
		# print "{} {} from {:d}".format(state, msg, pid)
		if state == ABORT:
			abort(msg, state_resp=True)
			return
		elif state == COMMIT:
			commit(msg, state_resp=True)
			return
		elif state == UNCERTAIN:
			uncertain_cmd = msg
			uncertain_pids.add(pid)
		else:
			has_committable = True
	if coor_state == COMMIT:
		commit(val, state_resp=True)
		return
	if not has_committable:
		# all uncertain, abort
		abort(uncertain_cmd, state_resp=True)
	else:
		for pids in uncertain_pids:
			# send precommit
			msg = "STATERESP {} {}".format(PRECOMMIT, uncertain_cmd)
			reply(pid, msg)
			time.sleep(SLEEP)
		commit(uncertain_cmd, state_resp=True)

def broadcast(msg):
	global clients, alives
	for pid, client in clients.items():
		if pid in alives:
			try:
				client.send(msg)
			except:
				pass


def commit(msg, state_resp=False):
	global crash, DTlog, states, COMMIT, localstate, self_pid, partialCommitIDs
	message = "COMMIT {}\n".format(msg)
	DTlog.append(message)
	localstate = COMMIT
	if state_resp:
		message = 'STATERESP ' + message
		for pid in states:
			print "{:d} Reply {} to {:d}".format(self_pid, message, pid)
			reply(pid, message)
		return
	if crash.crashPartialCommit:
		crash.crashPartialCommit = False
		for pid in partialCommitIDs:
			reply(pid, message)
		exit()
	else:
		broadcast(message)


def pre_commit(msg, state_resp=False):
	global crash, states, partialPrecommitIDs
	message = "PRECOMMIT {}\n".format(msg)
	if state_resp:
		message = 'STATERESP ' + message
		for pid in states:
			reply(pid, message)
		return
	if crash.crashPartialPreCommit:
		crash.crashPartialPreCommit = False
		for pid in partialPrecommitIDs:
			print "{:d} to {:d} {}".format(self_pid, pid, message)
			reply(pid, message)
		exit()
	else:
		broadcast(message)

def abort(command, state_resp=False):
	global votes, alives, clients, localstate, states, DTlog
	message = "ABORT {}\n".format(command)
	DTlog.append(message)
	localstate = ABORT
	if state_resp:
		message = 'STATERESP ' + message
		for pid in states:
			reply(pid, message)
	else:
		# send abort to those who voted 
		for key in votes: 
			if key in alives and votes[key]: 
				reply(key, message)


def vote_req(msg):
	global voteREQIDs, vote_reqs, alives, isCoordinator, votes, crash
	votes = {}
	message = "VOTEREQ {}".format(msg)
	if crash.crashVoteREQ:
		vote_reqs = voteREQIDs
		crash.crashVoteREQ = False
		for pid in voteREQIDs:
			reply(pid, message)
		exit()
	else:
		vote_reqs = set(alives.keys())
		broadcast(message)


def reply(target_pid, msg):
	global clients	
	clients[target_pid].send(msg)


def write_DTlog():
	global DTlog, DT_PATH, alives
	with open(DT_PATH, 'wt') as file:
		# Write all alive processes
		alive_ids = [str(pid) for pid in alives]
		file.write('ALIVES {}\n'.format(' '.join(alive_ids)))
		for line in DTlog:
			if not line.endswith('\n'):
				line = line + '\n'
			file.write(line)

def load_DTlog():
	global DTlog, DT_PATH
	try:
		with open(DT_PATH, 'rt') as file:
			DTlog = [line for line in file if line]
		alives_before = set([int(val) for val in DTlog[0].split() if val != 'ALIVES'])
		# TODO: send state request to all alives
		# Wait for response before restarting
	except:
		pass

# Master -> commands to coordinator or ask the server to crash
# n listeners and n clients:
def main(pid, num_servers, port):
	global master_thread, DT_PATH, self_pid, max_num_servers, countDown, wait, crash
	self_pid = pid
	max_num_servers = num_servers
	DT_PATH = "DTlogs/{:d}_DTlog.txt".format(pid)
	load_DTlog()
	countDown = CountDown()
	wait = Wait()
	crash = Crash()
	master_thread = MasterListener(pid, num_servers, port)
	master_thread.start()
	timeout_thread = Thread(target=timeout, args=())
	timeout_thread.setDaemon(True)
	timeout_thread.start()

# Call this function to exit the program
def exit():
	global isCoordinator, listeners, clients, master_thread
	if isCoordinator:
		isCoordinator = False
		run_election()
	for key in listeners:
		listeners[key].kill()
	for key1 in clients:
		clients[key1].kill() 
	master_thread.kill()
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