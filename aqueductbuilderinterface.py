import json
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
	lib.upload('temp.tar.gz', builder_address+'/build/submit', data)



def queue_tasks(tasks):
	for task in tasks:
		queue.enqueue(task)



def pick_builder(arch, os, release, urgency='low'):
	address = ''
	builder = db.get_free_builder_supporting_release(arch, os, release)
	if not builder:
		builder = db.get_free_builder(arch, os)
	return builder





class builder_monitor(threading.Thread):

	def __init__ (self, q, metaclass=lib.Singleton):
		threading.Thread.__init__(self)

	def run(self):
		while True:
			builders = db.get_all_builders()
			for b in builders:
				result = lib.download(b['address'])
				if result:
					info = json.loads(str(result)[2:].rstrip("'"))
					#print(info)
					db.mark_builder_online(b['address'], b['fingerprint'], 1)
					db.update_builder(b['address'], b['fingerprint'], info['name'], info['arch'], info['os'], [])
					db.delete_old_assignments(b['address'], b['fingerprint'])
					for task in db.get_tasks_assigned_to_builder(b['address'], b['fingerprint']):
						if task != info['building'] and task not in info['queue']:
							print("Builder dropped task, unassigning")
							db.unassign_task_from_builder(b['address'], b['fingerprint'], task['jobid'], task['arch'], task['os'], task['release'])
							queue.enqueue(task)
				else:
					db.mark_builder_online(b['address'], b['fingerprint'], 0)
					db.unassign_tasks_from_builder(b['address'], b['fingerprint'])
			time.sleep(60)



class queue_monitor(threading.Thread):

	def __init__ (self, q, metaclass=lib.Singleton):
		self.q = q
		tasks = db.get_unassigned_tasks()
		for task in tasks:
			self.q.enqueue(task)
		threading.Thread.__init__(self)

	def run(self):
		while True:
			data = []
			task,score = self.q.dequeue_with_priority()

			while True: #For each task in the queue, try to find a builder
				#print(task)
				builder = pick_builder(task['arch'], task['os'], task['release'])
				if builder:
					assign_build(builder['address'], builder['fingerprint'], task['jobid'], task['arch'], task['os'], task['release'], task['source'])
				else:
					data.append((task, score))
				try:
					task,score = self.q.dequeue_with_priority(block=False)
				except Empty:
					break

			for x in data: #Put the unassigned tasks back in the queue
				self.q.enqueue_with_priority(x[0], x[1])
			time.sleep(20)



bmonitor = builder_monitor(queue)
bmonitor.start()
qmonitor = queue_monitor(queue)
qmonitor.start()
