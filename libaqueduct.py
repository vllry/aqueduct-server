#!/usr/bin/python3

from os import listdir, path, remove
import shutil
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
		for d in self.general['dir']: #Ensure all dirs have a trailing slash
			if self.general['dir'][d][-1] != '/':
				self.general['dir'][d] = self.general['dir'][d] + '/'
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



def intake(conf, filepath):
	name = untar(filepath, conf.general['dir']['processing'])
	filepath = conf.general['dir']['processing'] + name + '/'
	if path.isfile(filepath):
		print("Tarfile did not have valid contents")
		return
	Aqueduct = json.load(open(filepath+'debian/Aqueduct', 'r'))

	for operatingsystem in Aqueduct['oses']:
		for release in Aqueduct['oses'][operatingsystem]['releases'].replace(' ','').split(','):
			#shutil.copytree(filepath, '%s%s_%s_%s' % (conf.general['dir']['processing'], name, operatingsystem, release))
			print(release)
