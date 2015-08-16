import json
from queue import Empty
import time
import threading

import aqueductdatabase as db
import libaqueduct as lib
from libaqueductserver import Config



conf = Config()
queue = lib.PriorityQueue()



class Builder:
	def __init__(self, address, fingerprint):
		self.address = address
		self.fingerprint = fingerprint
		self._online = None
		self._label = None
		self._arch = None
		self._os = None
		self._releases = None

	def online(self, current=None):
		if self._online is None:
			self._online = db.is_builder_online(self.address, self.fingerprint)
		if current is not None:
			self._online = current
			db.mark_builder_online(self.address, self.fingerprint, int(current))
		return self._online

	def label(self, current=None):
		if self._label is None:
			self._label = db.get_builder_attribute(self.address, self.fingerprint, 'label')
		if current is not None:
			if self._label != current:
				self._label = current
				db.set_builder_attribute(self.address, self.fingerprint, 'label', current)
		return self._label

	def arch(self, current=None):
		if self._arch is None:
			self._arch = db.get_builder_attribute(self.address, self.fingerprint, 'arch')
		if current is not None:
			if self._arch != current:
				self._arch = current
				db.set_builder_attribute(self.address, self.fingerprint, 'arch', current)
		return self._arch

	def os(self, current=None):
		if self._os is None:
			self._os = db.get_builder_attribute(self.address, self.fingerprint, 'os')
		if current is not None:
			if self._os != current:
				self._os = current
				db.set_builder_attribute(self.address, self.fingerprint, 'os', current)
		return self._os

	def releases(self, current=None):
		if self._releases is None:
			self._releases = db.get_builder_releases(self.address, self.fingerprint)
		if current is not None:
			for r in self._releases:
				if r not in current:
					db.remove_builder_release(self.address, self.fingerprint, r)
			for r in current:
				if r not in self._releases:
					db.add_builder_release(self.address, self.fingerprint, r)
			self._releases = current
		return self._releases

	def tasks(self):
		return db.get_tasks_assigned_to_builder(self.address, self.fingerprint)

	def assign(self, task):
		db.assign_task(self.address, self.fingerprint, task.jobid, task.arch, task.os, task.release)
		conf.print('info', "Task submitted to " + self.address)
		lib.targz('%s%s/%s/%s' % (conf.general['dir']['processing'], str(task.jobid), task.os, task.release), 'temp.tar.gz')
		data = {
			'callbackurl' : 'http://localhost:6500/callback',
			'jobid' : task.jobid,
			'arch' : task.arch,
			'os' : task.os,
			'release' : task.release
		}
		lib.upload('temp.tar.gz', self.address+'/build/submit', data)

	def unassign(self, task):
		db.unassign_task_from_builder(self.address, self.fingerprint, task.jobid, task.arch, task.os, task.release)
		queue.enqueue(task.dict())

	def unassign_all(self):
		tasks = self.tasks()
		db.unassign_tasks_from_builder(self.address, self.fingerprint)
		for task in tasks:
			queue.enqueue(task)



class Task:
	def __init__(self, jobid, arch, os, release):
		self.jobid = jobid
		self.arch = arch
		self.os = os
		self.release = release

	def dict(self):
		return {
			'jobid': self.jobid,
			'arch': self.arch,
			'os': self.os,
			'release': self.release
		}



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
					builders.append(Builder(row[0], row[1]))
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
							b.unassign(Task(task['jobid'], task['arch'], task['os'], task['release']))
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
					b = Builder(builder['address'], builder['fingerprint'])
					b.assign(Task(task['jobid'], task['arch'], task['os'], task['release']))
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
