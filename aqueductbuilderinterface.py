from queue import Empty
import time
import threading

import aqueductdatabase as db
import libaqueduct as lib
import libaqueductserver as aqueduct



queue = lib.PriorityQueue()



def assign_build(builder_address, builder_fingerprint, jobid, build_arch, build_os, build_release, sourcedir):
	db.assign_task(builder_address, builder_fingerprint, jobid, build_arch, build_os, build_release)
	print("Task submitted to " + builder_address)
	lib.targz(sourcedir, 'temp.tar.gz')
	data = {
		'callbackurl' : 'http://localhost:6500/callback',
		'jobid' : jobid,
		'arch' : build_arch,
		'os' : build_os,
		'release' : build_release,
	}
	lib.upload('temp.tar.gz', builder_address, data)



def queue_tasks(tasks):
	for task in tasks:
		queue.put((task['jobid'], task['arch'], task['os'], task['release'], task['sourcedir']))



def pick_builder(arch, os, release, urgency='low'):
	address = ''
	builder = db.get_free_builder_supporting_release(arch, os, release)
	if not builder:
		builder = db.get_free_builder(arch, os)
	return builder





#class builder_monitor(threading.Thread):
#	while True:
#		builders = db.get_all_builders()
#		for builder in builders:
#			pass
#			#Check if up and what task(s) it has
#			#Check if task(s) were assigned
#			#Unassign tasks if relavent
#			#Update db info about builder id relavent
#		time.sleep(60)



class queue_monitor(threading.Thread):

	def __init__ (self, q, metaclass=lib.Singleton):
		self.q = q
		tasks = db.get_unassigned_tasks()
		for task in tasks:
			self.q.put((time.time(), task))
		threading.Thread.__init__(self)
   

	def run(self):
		while True:
			data = []
			score,tup = self.q.get()

			while tup: #For each task in the queue, try to find a builder
				print(tup)
				task = {'jobid' : tup[0], 'build_arch' : tup[1], 'build_os' : tup[2], 'build_release' : tup[3], 'sourcedir' : tup[4]}
				builder = pick_builder(task['build_arch'], task['build_os'], task['build_release'])
				if builder:
					assign_build(builder['address'], builder['fingerprint'], task['jobid'], task['build_arch'], task['build_os'], task['build_release'], task['sourcedir'])
				else:
					data.append((score, tup))
				try:
					score,tup = self.q.queue.get(block=False)
				except Empty:
					break

			for x in data: #Put the unassigned tasks back in the queue
				self.q.queue.put(x)
			time.sleep(20)



monitor = queue_monitor(queue)
monitor.start()
