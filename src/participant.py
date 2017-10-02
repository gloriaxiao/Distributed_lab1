#!/usr/bin/env python
"""
The participant program for CS5414 three phase commit project.
"""
import sys

def main(pid, num_servers, port):
	pass

if __name__ == '__main__':
	aargs = sys.argv
	if len(args) != 4:
		print("Need three arguments!")
		os._exit(0)
	try:
		main(int(args[1]), int(args[2]), int(args[3]))
	except KeyboardInterrupt: 
		os._exit(0)