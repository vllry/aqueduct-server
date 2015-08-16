import json
from os import listdir, path, remove
from random import randrange
import tarfile
import time

import aqueductbuilderinterface as builder_interface
import aqueductdatabase as db



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
