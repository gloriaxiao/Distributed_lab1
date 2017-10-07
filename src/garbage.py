# elif wait.waitForACK:
# 	print "Timeout waiting for ACKs"
# 	wait.waitForACK = False
# 	ackIDs = set()
# 	finish_commit(wait.waiting_cmd)
# 	ack_reqs = {}
# 	wait.waiting_cmd = ""

# elif wait.waitForACK:
# 	if not ack_reqs - ackIDs:
# 		print "get all ACKs"
# 		wait.waitForACK = False
# 		ackIDs = set()
# 		countDown.stop()
# 		starttime = 0
# 		finish_commit(wait.waiting_cmd)
# 		wait.waiting_cmd = ""