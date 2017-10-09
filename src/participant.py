#!/usr/bin/env python
"""
The participant program for CS5414 three phase commit project.
"""
import os, errno
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
COOR_HEARTBEAT = "coor_heartbeat"
ELECT = "URELECT"
START_REQ = "STARTREQ"
START_RESP = "STARTRESP"

# Global Variables
DTlog = []
playlist = {}
DT_PATH = ""

# ID sets
voteREQIDs = set()
partialCommitIDs = set()
partialPreCommitIDs = set()

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
# container class variables
wait = None
countDown = None
crash = None

# start Variable
wait_for = set()
wait_to_start = set()
blocked = False

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
		self.waitForState = False
		self.waitForStateReq = False
		self.waitForStateResp = False
		self.waitForCommit = False
		self.waitForPreCommit = False
		self.waitForACK = False
		self.waiting_cmd = ""

	def finished_waiting(self):
		wait = self.waitForVote + self.waitForState + \
		self.waitForStateReq + self.waitForStateResp + self.waitForCommit + \
		self.waitForPreCommit + self.waitForACK
		return (not wait)

	def reset(self):
		self.waitForVote = False
		self.waitForState = False
		self.waitForStateReq = False
		self.waitForStateResp = False
		self.waitForCommit = False
		self.waitForPreCommit = False
		self.waitForACK = False
		self.waiting_cmd = ""


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
		global alives, isCoordinator, DTlog, clients, votes, playlist, crash, COOR_ID, in3PC
		global partialCommitIDs, partialPreCommitIDs, voteREQIDs, blocked, localstate

		if len(alives) == 0:
			isCoordinator = True
			COOR_ID = self.pid
			print "coordinator {:d}".format(self.pid)
			self.master_conn.send("coordinator {:d}\n".format(self.pid))
		restore_state()
		while self.connected:
			if '\n' in self.buffer and not blocked:
				(l, rest) = self.buffer.split("\n", 1)
				print "{:d} receives {} from master".format(self.pid, l)
				self.buffer = rest
				cmd = l.split()[0]
				if cmd == "get":
					_, msgs = l.split(None, 1)
					url = playlist.get(msgs.strip(), 'NONE')
					self.master_conn.send("resp {}\n".format(url))
				elif cmd == "crashAfterVote":
					crash.crashAfterVote = True
				elif cmd == "crashBeforeVote":
					crash.crashBeforeVote = True
				elif cmd == "crashAfterAck":
					crash.crashAfterAck = True 
				elif cmd == "crash":
					exit()
					break
				# For coordinator only
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
						partialPreCommitIDs = set([int(val) for val in msgs.split()])
				elif cmd == "crashPartialCommit" and isCoordinator:
					crash.crashPartialCommit = True 
					if l == cmd:
						partialCommitIDs = set()
					else:
						_, msgs = l.split(None, 1)
						partialCommitIDs = set([int(val) for val in msgs.split()])
				elif cmd == "add" or cmd == "delete":
					print "run 3pc protocol"
					in3PC = True
					self.start_VoteREQ(l)
					if crash.crashBeforeVote:
						crash.crashBeforeVote = False
						exit()
						break
					# local decision:
					args = l.split()
					if cmd == "add" and len(args[2]) > self.pid + 5:
						print " Coordinator voted no: " + l
						localstate = ABORT
						DTlog.append("ABORT {}".format(l))
					else:
						DTlog.append("YES {}".format(l))
						localstate = UNCERTAIN
					if crash.crashAfterVote:
						crash.crashAfterVote = False 
						countDown.stop()
						exit()
						break
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
		global votes, vote_reqs, alives, wait, DTlog
		# Keep track of the set of alive process at the time of VOTEREQ
		vote_req(l)
		wait.waitForVote = True
		wait.waiting_cmd = l
		countDown.begin()

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
		global countDown, wait, crash, DTlog, died_coor, isCoordinator, wait_for, blocked
		global localstate, COOR_ID, alives, playlist, in3PC, states, wait_to_start
		self.conn, self.addr = self.sock.accept()
		self.connected = True 
		while True: 
			if "\n" in self.buffer: 
				(l, rest) = self.buffer.split("\n", 1)
				self.buffer = rest
				msgs = l.split()
				if msgs[0] == HEARTBEAT:
					if self.target_pid in wait_to_start:
						try:
							del alives[self.target_pid]
						except:
							pass
					else:
						alives[self.target_pid] = time.time()
				elif msgs[0] == COOR_HEARTBEAT:
					COOR_ID = self.target_pid
					alives[self.target_pid] = time.time()
				elif msgs[0] == STATEREQ:
					if COOR_ID != self.target_pid:
						# didn't detect the failure of Coordinator
						if self.target_pid not in died_coor:
							remove_zombies(self.target_pid)
							COOR_ID = self.target_pid
						else:
							# ignore request from zombie coordinator
							continue
					wait.reset()
					if not DTlog:
						continue
					_, command = DTlog[-1].split(None, 1)
					reply(self.target_pid, "STATE {} {}".format(localstate, command))
					wait.waitForStateResp = True
					countDown.begin()
				elif msgs[0] == VOTEREQ:
					_, info = l.split(None, 1)
					in3PC = True
					cmd = msgs[1]
					if crash.crashBeforeVote:
						crash.crashBeforeVote = False 
						DTlog.append("ABORT {}".format(info))
						exit()
						break
					vote_yes = True
					if cmd == "add":
						name, url = msgs[2], msgs[3]
						if len(url) > self.pid + 5:
							DTlog.append("ABORT {}".format(info))
							print "{:d} respond No on {}".format(self.pid, info)
							reply(self.target_pid, "NO {:d}".format(self.pid))
							vote_yes = False
							localstate = ABORT
							end_3PC()
					if vote_yes:
						DTlog.append("YES {}".format(info))
						print "{:d} respond Yes on {}".format(self.pid, info)
						reply(self.target_pid, "YES {:d}".format(self.pid))
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
				elif msgs[0] == COMMIT:
					print "{:d} get commit".format(self.pid)
					wait.waitForCommit = False
					_, info = l.split(None, 1)
					DTlog.append(l)
					if msgs[1] == 'add':
						name, url = msgs[2], msgs[3]
						playlist[name] = url 
					elif msgs[1] == 'delete':
						try:
							del playlist[msgs[2]]
						except:
							pass
					localstate = COMMIT
					end_3PC()
				elif msgs[0] == "STATERESP":
					wait.waitForStateResp = False
					_, info = l.split(None, 1)
					cmd, message = info.split(None, 1)
					statelog = info
					if (cmd == ABORT or COMMIT):
						if statelog not in DTlog:
							DTlog.append(statelog)
						if localstate == COMMITTABLE: 
							if cmd == ABORT: 
								pass 
							else: 
								data_pieces = message.split()
								if data_pieces[0] == 'add': 
									playlist[data_pieces[1]] = data_pieces[2]
								elif data_pieces[0] == 'delete':
									try: 
										del playlist[data_pieces[1]]
									except: 
										pass
								end_3PC()
					else:
						reply(self.target_pid, "ACK {:d}\n".format(self.pid))
						wait.waitForCommit = True
						wait.waiting_cmd = message
						countDown.begin()
				elif msgs[0] == "NO":
					votes[self.target_pid] = False
				elif msgs[0] == "YES": 
					votes[self.target_pid] = True
				elif msgs[0] == "ACK":
					if wait.waitForACK:
						wait.waitForACK = False
						finish_commit(wait.waiting_cmd)
						wait.waiting_cmd = ""
				elif msgs[0] == "STATE":
					_, info = l.split(None, 1)
					states[self.target_pid] = info
				elif msgs[0] == ELECT:
					isCoordinator = True
					COOR_ID = self.pid
					print "receive Election " + str(self.pid)
					master_thread.master_conn.send("coordinator {:d}\n".format(self.pid))	
					start_STATEREQ()
				elif msgs[0] == START_REQ:
					if in3PC:
						wait_to_start.add(self.target_pid)
					else:
						if blocked:
							alives[self.target_pid] = time.time()
							continue
						log = ','.join(DTlog)
						print "{:d} respond to start request from {:d} with {}".format(self.pid, self.target_pid, "{} {}".format(START_RESP, log))
						reply(self.target_pid, "{} {}".format(START_RESP, log))
				elif msgs[0] == START_RESP:
					if l.strip() == START_RESP:
						blocked = False
					else:
						_, msg = l.split(None, 1)
						logs = msg.split(',')
						process_received_log(logs)
				elif msgs[0] == 'STARTWAIT':
					_, ids = l.split(None, 1)
					wait_to_start = set([int(v) for v in ids.split() if v])
			else:
				try: 
					data = self.conn.recv(1024)
					if data == "": 
						raise ValueError
					self.buffer += data 
				except Exception as e:
					print str(self_pid) + " to " + str(self.target_pid) + " connection closed"
					if wait.waitForState:
						states[self.target_pid] = ""
					self.conn.close()
					self.conn = None 
					self.conn, self.addr = self.sock.accept()

	def kill(self):
		try:
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
  	global isCoordinator, blocked, wait_for, alives
  	while True:
  		if blocked and self.target_pid in wait_for and self.target_pid in alives:
			self.send(START_REQ)
  		elif isCoordinator:
  			self.send(COOR_HEARTBEAT)
  		else:
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


def process_received_log(log):
	global DTlog, blocked, playlist, localstate, wait_for
	local_cmds = set()
	for line in DTlog:
		if not line:
			continue
		state, cmd = line.split(None, 1)
		if state == COMMIT or state == ABORT:
			local_cmds.add(cmd)
	for line in log:
		if not line:
			continue
		print "line: " + line 
		state, cmd = line.split(None, 1)
		print "state: " + state + " cmd: " + cmd 
		if cmd not in local_cmds:
			DTlog.append(line)
			if state == COMMIT:
				localstate = COMMIT
				# Commit on cmd
				op, info = cmd.split(None, 1)
				print op + " " + info 
				if op == 'add':
					# print str(self_id) + " adding to list"
					name, url = info.split()
					playlist[name] = url
				else:
					try:
						del playlist[info]
					except:
						pass
			elif state == ABORT:
				localstate = ABORT
			else:
				localstate = UNCERTAIN
	blocked = False
	if isCoordinator:
		start_STATEREQ()


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
	global crash, votes, master_thread, DTlog, playlist, countDown
	global ack_reqs, wait, localstate, alives, in3PC, ABORT, localstate
	if False in votes.values() or localstate == ABORT:
		localstate = ABORT
		votes = {}
		master_thread.master_conn.send("ack ABORT\n")
		abort(l)
		return
	# all Yes
	votes = {}
	# precommit and commit 
	pre_commit(l)
	localstate = COMMITTABLE
	if crash.crashAfterAck:
		crash.crashAfterAck = False
		exit()
		return
	wait.waiting_cmd = l
	wait.waitForACK = True
	countDown.begin()


def finish_commit(l, state_resp=False):
	global master_thread, playlist, wait_to_start, localstate
	localstate = COMMIT
	master_thread.master_conn.send("ack COMMIT\n")
	message = "COMMIT {}".format(l)
	args = l.split()
	cmd, name = args[0], args[1]
	if cmd == "add":
		playlist[name] = args[2]
	else:
		try: 
			del playlist[name]
		except:
			pass
	if message not in DTlog:
		DTlog.append(message)
	commit(message, state_resp=state_resp)
	end_3PC()


def end_3PC():
	global in3PC, wait_to_start, START_RESP, DTlog
	in3PC = False
	for pid in wait_to_start:
		log = ''.join(DTlog)
		reply(pid, "{} {}".format(START_RESP, log))
	wait_to_start = set()


def timeout():
	global wait, countDown, votes, vote_reqs, self_pid, master_thread, DTlog
	starttime = 0
	while True:
		if countDown.start:
			if not starttime:
				# set the start time of the countdown process
				starttime = time.time()
			elif (time.time() - starttime) > TIMEOUT:
				countDown.stop()
				starttime = 0 
				if wait.waitForState:
					print str(self_pid) + " Timeout waiting for State Info"
					wait.waitForState = False
					run_3PC_termination()
				elif wait.waitForStateReq:
					print str(self_pid) + " Timeout waiting for State Request"
					wait.waitForStateReq = False
					run_election()
				elif wait.waitForStateResp:
					print str(self_pid) + " Timeout waiting for State Response from Coor" + str(COOR_ID)
					wait.waitForStateResp = False
					run_election()
				elif wait.waitForVote:
					print str(self_pid) + " Timeout waiting for votes"
					wait.waitForVote = False
					master_thread.master_conn.send("ack ABORT\n")
					abort(wait.waiting_cmd)
					votes = {}
				elif wait.waitForPreCommit:
					print str(self_pid) + " Timeout waiting for precommit"
					wait.waitForPreCommit = False
					localstate = UNCERTAIN
					run_election()
				elif wait.waitForCommit:
					print str(self_pid) + " Timeout waiting for commit"
					wait.waitForCommit = False
					localstate = COMMITTABLE
					run_election()
				elif wait.waitForACK:
					print str(self_pid) + " Timeout waiting for ACK"
					wait.waitForACK = False
					finish_commit(wait.waiting_cmd)
				wait.waiting_cmd = ""
			elif wait.waitForVote:
				if not (vote_reqs - set(votes.keys())):
					print str(self_pid) + " get all votes"
					cmd = wait.waiting_cmd
					wait.waitForVote = False
					wait.waiting_cmd = ""
					countDown.stop()
					starttime = 0
					run_3PC(cmd)
			elif wait.waitForState:
				if not (state_reqs - set(states.keys())):
					print str(self_pid) + " get all states"
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
	global ELECT, isCoordinator, wait_to_start, master_thread
	# if COOR_ID in alives: 
	# 	return
	new_coor = (COOR_ID + 1) % max_num_servers
	died_coor.add(COOR_ID)
	if COOR_ID in alives:
		del alives[COOR_ID]
	while new_coor != COOR_ID: 
		if new_coor in alives:
			if isCoordinator and wait_to_start:
				# Transfer all waiting to start processes
				msg = 'STARTWAIT {}'.format(' '.join([str(v) for v in wait_to_start]))
				clients[new_coor].send(startWAIT)
			clients[new_coor].send(ELECT)
			if new_coor in died_coor:
				died_coor.remove(new_coor)
			COOR_ID = new_coor
			wait.waitForStateReq = True
			break
		if new_coor == self_pid:
			isCoordinator = True
			COOR_ID = self_pid
			print "{:d} Elected as Coordinator".format(self_pid)
			master_thread.master_conn.send("coordinator {:d}\n".format(self_pid))	
			start_STATEREQ()
			break
		new_coor = (new_coor + 1) % max_num_servers 


def start_STATEREQ():
	global state_reqs, self_pid, STATEREQ, wait, countDown
	print "send state requests"
	state_reqs = set(alives.keys())
	print state_reqs
	broadcast(STATEREQ)
	wait.waitForState = True
	countDown.begin()


def run_3PC_termination():	
	# If any state report is aborted, coordinate writes and send abort
	global DTlog, localstate, states, master_thread, self_pid
	uncertains = set()
	cmd = ""
	if DTlog:
		statelog = DTlog[-1]
		coor_state, cmd = statelog.split(None, 1)
		if coor_state == ABORT:
			abort(cmd, state_resp=True)
			return
		if coor_state == COMMIT:
			finish_commit(cmd, state_resp=True)
			return
	uncertain_pids = set()
	has_committable = (localstate == COMMITTABLE)
	for pid, info in states.items():
		if not info:
			continue
		state, cmd = info.split(None, 1)
		if state == ABORT:
			master_thread.master_conn.send('ack ABORT\n')
			abort(cmd, state_resp=True)
			return
		elif state == COMMIT:
			finish_commit(cmd, state_resp=True)
			return
		elif state == UNCERTAIN:
			uncertain_pids.add(pid)
		else:
			has_committable = True
	if not cmd:
		return
	if not has_committable:
		# all uncertain, abort
		master_thread.master_conn.send('ack ABORT\n')
		abort(cmd, state_resp=True)
	else:
		for pids in uncertain_pids:
			msg = "STATERESP {} {}".format(PRECOMMIT, cmd)
			reply(pid, msg)
			time.sleep(SLEEP)
		finish_commit(cmd, state_resp=True)


def broadcast(msg):
	global clients, alives
	for pid, client in clients.items():
		if pid in alives:
			try:
				client.send(msg)
			except:
				pass


def commit(msg, state_resp=False):
	global crash, states, partialCommitIDs
	if state_resp:
		msg = 'STATERESP ' + msg
		for pid in states:
			reply(pid, msg)
		return
	if crash.crashPartialCommit:
		crash.crashPartialCommit = False
		for pid in partialCommitIDs:
			reply(pid, msg)
		exit()
	else:
		broadcast(msg)


def pre_commit(msg, state_resp=False):
	global crash, states, partialPreCommitIDs
	if not msg.endswith('\n'):
		msg = msg + '\n'
	message = "PRECOMMIT {}".format(msg)
	if state_resp:
		message = 'STATERESP ' + message
		for pid in states:
			reply(pid, message)
		return
	if crash.crashPartialPreCommit:
		crash.crashPartialPreCommit = False
		for pid in partialPreCommitIDs:
			reply(pid, message)
		exit()
	else:
		broadcast(message)


def abort(command, state_resp=False):
	global votes, alives, clients, localstate, states, DTlog
	message = "ABORT {}".format(command)
	if message not in DTlog:
		DTlog.append(message)
	localstate = ABORT
	end_3PC()
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
	global clients, self_pid
	clients[target_pid].send(msg)


def write_DTlog():
	global DTlog, DT_PATH, alives, localstate
	with open(DT_PATH, 'wt') as file:
		alive_ids = [str(pid) for pid in alives]
		# Write all alive processes
		file.write('ALIVES {}\n'.format(' '.join(alive_ids)))
		for line in DTlog:
			file.write(line + '\n')


def restore_state():
	global DTlog, DT_PATH, wait_for, isCoordinator, localstate, blocked, alives, self_pid
	try:
		with open(DT_PATH, 'rt') as file:
			aliveline = file.readline()
			try:
				_, aliveset = aliveline.split(None, 1)
				wait_for = set([int(val) for val in aliveset.split() if val])
			except:
				pass
			for line in file:
				if line.endswith('\n'):
					line = line[:-1]
				DTlog.append(line)
	except:
		pass

	if isCoordinator and not DTlog:
		blocked = False
		return
	# Restart or participant join later
	else:
		if DTlog:
			process_local_log()
			statelog = DTlog[-1]
			cmd, msg = statelog.split(None, 1)
			if cmd == COMMIT or cmd == ABORT:
				localstate = cmd
			else:
				localstate = UNCERTAIN
			if not wait_for:
				blocked = False
				return
		if not isCoordinator:
			reply(COOR_ID, START_REQ)


def process_local_log():
	global playlist, DTlog, localstate
	for log in DTlog:
		cmd, msg = log.split(None, 1)
		if cmd == COMMIT:
			infos = msg.split()
			if infos[0] == 'add':
				name, url = infos[1], infos[2]
				playlist[name] = url
			else:
				try:
					del playlist[infos[1]]
				except:
					pass


def make_sure_path_exists(path):
	try:
		os.makedirs(path)
	except OSError as e:
		if e.errno != errno.EEXIST:
			print("Error: Path couldn't be recognized!")
			print(e)


# Master -> commands to coordinator or ask the server to crash
def main(pid, num_servers, port):
	global master_thread, DT_PATH, self_pid, max_num_servers, countDown, wait, crash, blocked
	self_pid = pid
	max_num_servers = num_servers
	DT_PATH = "DTlogs/log{:d}.txt".format(pid)
	make_sure_path_exists("DTlogs")
	countDown = CountDown()
	wait = Wait()
	crash = Crash()
	blocked = True
	master_thread = MasterListener(pid, num_servers, port)
	master_thread.start()
	timeout_thread = Thread(target=timeout, args=())
	timeout_thread.setDaemon(True)
	timeout_thread.start()


# Call this function to exit the program
def exit():
	global isCoordinator, listeners, clients, master_thread
	if isCoordinator:
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