#!/usr/bin/python3

from os import listdir, path
import tarfile
import json



class config:
	"""Load and process the config files"""


	def __init__(self, config_file_path = "/home/vallery/Development/Aqueduct/aqueduct-server/etc/aqueduct-server/aqueduct-server.conf"):
		attributes = [
						'arch',
						'components',
						'distributions'
					]

		self.repos = {}
		self.general = json.load(open(config_file_path, 'r'))
		repo_conf = json.load(open(self.general['repositories'], 'r'))

		for repo in repo_conf:
			self.repos[repo] = {}
			for release in repo_conf[repo]['releases']:
				self.repos[repo][release] = repo_conf[repo]['releases'][release]
				for attribute in attributes:
					if attribute not in self.repos[repo][release] and attribute in repo_conf[repo]['defaults']:
						self.repos[repo][release][attribute] = repo_conf[repo]['defaults'][attribute]



def var_replace(s, values_original):
	"""Replaces all instances in s (regardless of case) of {{KEY}} with values_original[KEY]"""

	values = {}
	for key in values_original.keys(): #Add brackets around key values
		values['{{'+key+'}}'] = values_original[key]

	s_lower = s.lower()
	for key in values.keys():
		pos = s_lower.find(key)
		while pos != -1:
			old = s[pos:pos+len(key)]
			s = s.replace(old, values[key])
			s_lower = s.lower()
			pos = s_lower.find(key)
	return s



def untar(filepath, dest):
	tfile = tarfile.open(filepath, 'r:gz')
	tfile.extractall(dest)
	name = tfile.getnames()[0]
	tfile.close() #?
	remove(filepath)
	return name



def intake(filepath):
	filepath = untar(filepath, conf.general['dir']['processing'])
	if filepath.isfile():
		print("Tarfile did not have valid contents")
		return
	if filepath[-1] != '/':
		filepath = filepath + '/'
	f = open(filepath+'debian/Aqueduct', 'r')
	Aqueduct = f.read()
	f.close()

	
