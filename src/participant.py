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
BUFFER_SIZE = 256
BASE_PORT = 20000
ADDR = 'localhost'
playlist = {}
listeners = {}
isCoordinator = False
clients = {}
alives = set()
DTlog = []

# msg type
VOTEREQ = "VOTEREQ"
COMMIT = "COMMIT"
PRECOMMIT = "PRECOMMIT"
ABORT = "ABORT"


class MasterListener(Thread):
	def __init__(self, pid, port):
		global alives
		Thread.__init__(self)
		self.pid = pid
		self.port = port
		self.buffer = ""
		self.socket = socket(AF_INET, SOCK_STREAM)
		self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.socket.bind((ADDR, self.port))
		self.socket.listen(1)
		

	def run(self):
		global alives, isCoordinator, DTlog
		self.master_sock, _ = self.socket.accept()
		self.connected = True
		# First participant
		if not alives:
			sys.stdout.write('not alive processes')
			sys.stdout.flush()
			isCoordinator = True
			self.master_sock.send("coordinator {:d}\n".format(self.pid))
		else:
			sys.stdout.write(alives)
			sys.stdout.flush()
		while self.connected:
			if '\n' in self.buffer:
				(l, rest) = self.buffer.split("\n", 1)
				self.buffer = rest
				msgs = l.split(None, 1)
				cmd = msgs[0].strip()
				if cmd == "get":
					url = playlist.get(msgs[1].strip(), 'NONE')
					self.master_sock.send("resp {}\n".format(url))
				elif cmd == "crash":
					self.kill()
					exit()
				elif cmd == "crashAfterVote":
					pass
				elif cmd == "crashBeforeVote":
					pass
				elif cmd == "crashAfterAck":
					pass
				# For coordinator only
				elif cmd == "add" and isCoordinator:
					broadcast("VOTEREQ {}\n".format(self.l))
					DTlog.append("Start-3PC")
					sys.stdout.write("Start-3PC")
					sys.stdout.flush()
					
				elif cmd == "delete" and isCoordinator:
					pass
				
				elif cmd == "crashVoteREQ":
					pass
				elif cmd == "crashPartialPreCommit":
					pass
				elif cmd == "crashPartialCommit":
					pass
				else:
					sys.stdout.write("Unknown command {}".format(l))
					sys.stdout.flush()
			else:
				try:
					self.buffer += self.master_sock.recv(BUFFER_SIZE)
				except:
					self.connected = False
					self.master_sock.close()
					break

	def kill(self):
		try:
			self.connected = False
			self.master_sock.close()
			self.socket.close()
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

def commit():
	if isCoordinator:
		broadcast(COMMIT)

def pre_commit():
	if isCoordinator:
		broadcast(PRECOMMIT)

def abort():
	if isCoordinator:
		broadcast(ABORT)

def reply(target_pid, msg):
	global clients
	clients[target_pid].send(msg)

# def timeout():
# 	global wait_ack
# 	time.sleep(TIMEOUT)
# 	if wait_ack:
# 		sys.stdout.write("Timeout!")
# 		abort()


class serverListener(Thread):

	def __init__(self, pid, port, target_pid):
		self.pid = pid
		self.listen_port = port
		self.sock = socket(AF_INET, SOCK_STREAM)
		self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.sock.bind((ADDR, port))
		self.sock.listen(1)
		sys.stdout.write("Starts listening on port {:d} for process {:d}\n".format(port, target_pid))
		sys.stdout.flush()
		self.buffer = ""

	def run(self):
		global DTlog, alives, COOR_ID
		self.conn_sock, addr = self.sock.accept()
		_, target_port = addr #For debugging
		alives.add(target_pid)
		self.connected = True
		while True:
			if "\n" in self.buffer:
				(l, rest) = self.buffer.split("\n", 1)
				self.buffer = rest
				msgs = l.split()
				# If the process is not the coordinator
				if msgs[0] == VOTEREQ:
					COOR_ID = target_pid
					if msgs[1] == 'add':
						name, url = msgs[2], msgs[3]
						if len(url) > self.pid+5:
							#Vote No
							reply(target_pid, "NO {:d}\n".format(self.pid))
							DTlog.append("ABORT on command {}".format(msgs[1:]))
						else:
							#Vote YES
							reply(target_pid, "YES {:d}\n".format(self.pid))
					elif msgs[1] == 'delete':
						pass
						# Don't know the rule for reaching a decision
				# elif msgs[0] == "YES":					
			else:
				try:
					data = self.conn_sock.recv(BUFFER_SIZE)
					self.buffer += data
				except:
					pass
	
	def kill(self):
		try:
			self.connected = False
			self.conn_sock.close()
			self.sock.close()
		except:
			pass


class serverClient(Thread):

	def __init__(self, pid, port, target_pid, target_port):
		global alives
		self.pid = pid
		self.port = port
		self.target_pid = target_pid
		self.target_port = target_port
		while True:
			try:
				new_socket = socket(AF_INET, SOCK_STREAM)
				new_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
				new_socket.bind((ADDR, self.port))
				new_socket.connect((ADDR, self.target_port))
				break
			except:
				pass
		self.sock = new_socket
		self.connected = True
		self.buffer = ""
		alives.add(self.target_pid)

	def run(self):
		while True:
			try:
				new_socket = socket(AF_INET, SOCK_STREAM)
				new_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
				new_socket.bind((ADDR, self.port))
				new_socket.connect((ADDR, self.target_port))
				break
			except:
				pass
		self.sock = new_socket
		self.connected = True
		self.buffer = ""
		alives.add(self.target_pid)

	def send(self, msg):
		if not msg.endswith("\n"):
			msg += '\n'
		self.sock.send(msg)

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
	global alive_ids
	base_port = BASE_PORT + pid*num_servers*2
	sys.stdout.write("BASE PORT for server {:d} is {:d}\n".format(pid, base_port))
	sys.stdout.flush()
	master_thread = MasterListener(pid, port)
	master_thread.start()
	sys.stdout.write("Start the master thread \n")
	sys.stdout.flush()
	for i in range(num_servers):
		if i == pid:
			continue
		listener = serverListener(pid, base_port+i, i)
		target_port = BASE_PORT + i*num_servers*2 + num_servers + pid
		client = serverClient(pid, port, i, target_port)
		listeners[i] = listener
		clients[i] = client
		listener.start()
		client.start()



if __name__ == '__main__':
	args = sys.argv
	if len(args) != 4:
		sys.stdout.write("Need three arguments!")
		sys.stdout.flush()
		os._exit(0)
	try:
		sys.stdout.write("start the program with {} {} {}\n".format(args[0], args[1], args[2]))
		sys.stdout.flush()
		main(int(args[1]), int(args[2]), int(args[3]))
	except KeyboardInterrupt: 
		os._exit(0)