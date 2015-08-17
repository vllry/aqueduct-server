import json
from queue import Empty
import time
import threading

import aqueductdatabase as db
import libaqueduct as lib
import libaqueductserver as aqueduct



conf = aqueduct.Config()
queue = lib.PriorityQueue()



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
		builders = [] #List of Builder() objects
		while True:
			db_builders = db.get_all_builders()
			for row in db_builders:
				if all(b.address != row[0] or b.fingerprint != row[1] for b in builders):
					conf.print('info', 'Found new builder in the database')
					builders.append(aqueduct.Builder(row[0], row[1]))
			for b in builders:
				if all(b.address != row[0] or b.fingerprint != row[1] for row in db_builders):
					conf.print('info', 'Noticed that builder was deleted from the database')
					builders.remove(b)

			db.delete_old_assignments()

			for b in builders:
				result = lib.download(b.address)
				if result:
					info = json.loads(str(result)[2:].rstrip("'")) #That's some ugly string cleanup
					b.online(True)
					b.label(info['name'])
					b.arch(info['arch'])
					b.os(info['os'])
					b.releases(info['releases'])

					for task in b.tasks():
						if info['building']:
							info['queue'].append(info['building'])
						if not any(
							str(task['jobid']) == t['jobid'] and
							task['arch'] == t['arch'] and
							task['os'] == t['os'] and
							task['release'] == t['release']
							for t in info['queue']
						):
							conf.print('info', 'Builder dropped task, unassigning')
							b.unassign(aqueduct.Task(task['jobid'], task['arch'], task['os'], task['release']))
				else:
					b.online(False)
					b.unassign_all()
			time.sleep(30)



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
				conf.print('debug', task)
				builder = pick_builder(task['arch'], task['os'], task['release'])
				if builder:
					b = aqueduct.Builder(builder['address'], builder['fingerprint'])
					b.assign(aqueduct.Task(task['jobid'], task['arch'], task['os'], task['release']))
				else:
					data.append((task, score))
				try:
					task,score = self.q.dequeue_with_priority(block=False)
				except Empty:
					break

			for x in data: #Put the unassigned tasks back in the queue
				print(x)
				self.q.enqueue_with_priority(x[0], x[1])
			time.sleep(20)



bmonitor = builder_monitor(queue)
bmonitor.start()
qmonitor = queue_monitor(queue)
qmonitor.start()
