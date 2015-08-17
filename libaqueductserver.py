import json
from os import listdir, path, remove
from random import randrange
import tarfile
import time

import aqueductdatabase as db
import libaqueduct as lib



class Config:
	"""Load and process the config files"""

	def __init__(self, config_file_path = "/home/vallery/Development/Aqueduct/aqueduct-server/etc/aqueduct-server/aqueduct-server.conf"):
		attributes = [
			'arch',
			'components',
			'distributions'
		]

		self.repos = {}
		self.general = json_file(config_file_path)
		for d in self.general['dir']: #Ensure all dirs have a trailing slash
			if self.general['dir'][d][-1] != '/':
				self.general['dir'][d] = self.general['dir'][d] + '/'
		repo_conf = json_file(self.general['repositories'])
		if self.general['loglevel'] == 'error':
			self.general['loglevel'] = 1
		elif self.general['loglevel'] == 'warn':
			self.general['loglevel'] = 2
		elif self.general['loglevel'] == 'info':
			self.general['loglevel'] = 3
		elif self.general['loglevel'] == 'debug':
			self.general['loglevel'] = 4
		else:
			self.general['loglevel'] = 0

		for repo in repo_conf:
			self.repos[repo] = {'releases':{}, 'alliases':repo_conf[repo]['alliases']}
			for release in repo_conf[repo]['releases']:
				self.repos[repo]['releases'][release] = repo_conf[repo]['releases'][release]
				for attribute in attributes:
					if attribute not in self.repos[repo]['releases'][release] and attribute in repo_conf[repo]['defaults']:
						self.repos[repo]['releases'][release][attribute] = repo_conf[repo]['defaults'][attribute]

	def print(self, kind, message):
		if (kind == 'debug' and self.general['loglevel'] >= 4) or (kind == 'info' and self.general['loglevel'] >= 3) or (kind == 'warn' and self.general['loglevel'] >= 2) or (kind == 'error' and self.general['loglevel']):
			print(kind.upper() + ': ' + str(message))



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



def json_file(filepath):
	f = open(filepath, 'r')
	data = json.load(f)
	f.close()
	return data



def replace(s, values_original):
	"""Replaces all instances in s (regardless of case) of {{KEY}} with values_original[KEY]"""

	values = {}
	for key in values_original.keys(): #Add brackets around key values
		values['{{'+key+'}}'] = values_original[key]

	s_lower = s.lower()
	for key in values.keys():
		pos = s_lower.find(key.lower())
		while pos != -1:
			old = s[pos:pos+len(key)]
			s = s.replace(old, values[key])
			s_lower = s.lower()
			pos = s_lower.find(key.lower())
	return s



def untar(filepath, dest):
	tfile = tarfile.open(filepath, 'r:gz')
	tfile.extractall(dest)
	name = tfile.getnames()[0]
	tfile.close() #?
	remove(filepath)
	return name



def package_modify(Aqueduct, dir_processing, var_dictionary):
	for target in Aqueduct['modify']:
		target_path = dir_processing + target
		f = open(target_path, 'r')
		data = f.read()
		f.close()
		f = open(target_path, 'w')
		f.write(replace(data, var_dictionary))
		f.close()



conf = Config()
queue = lib.PriorityQueue()
