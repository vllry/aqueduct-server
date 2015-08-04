import json
from os import listdir, path, remove
from random import randrange
import shutil
import tarfile

import aqueductbuilderinterface as builder_interface
import aqueductdatabase as db



def json_file(filepath):
	f = open(filepath, 'r')
	data = json.load(f)
	f.close()
	return data



class config:
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

		for repo in repo_conf:
			self.repos[repo] = {'releases':{}, 'alliases':repo_conf[repo]['alliases']}
			for release in repo_conf[repo]['releases']:
				self.repos[repo]['releases'][release] = repo_conf[repo]['releases'][release]
				for attribute in attributes:
					if attribute not in self.repos[repo]['releases'][release] and attribute in repo_conf[repo]['defaults']:
						self.repos[repo]['releases'][release][attribute] = repo_conf[repo]['defaults'][attribute]



#Note: case of keys IS sensative
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



def intake(conf, filepath):
	dir_processing = conf.general['dir']['processing'] + str(randrange(0,99999)) + '/'
	print('Processing package in ' + dir_processing)
	dir_processing_orig = dir_processing + 'original/'

	name = untar(filepath, dir_processing_orig)
	dir_processing_orig += name + '/'
	if path.isfile(dir_processing_orig):
		print("Tarfile did not have valid contents")
		return
	Aqueduct = json_file(dir_processing_orig+'debian/Aqueduct')


	if Aqueduct['version'] < 1:
		builds = []
		for operatingsystem in Aqueduct['oses']:
			var_dictionary = {'os' : operatingsystem}
			releases = replace(Aqueduct['oses'][operatingsystem]['releases'].replace(' ',''), conf.repos[operatingsystem]['alliases']).split(',')

			for release in releases:
				var_dictionary['release'] = release

				#Modify the source for this particular os/release
				shutil.copytree(dir_processing_orig, '%s%s_%s' % (dir_processing, operatingsystem, release))
				target_dir = "%s%s_%s/" % (dir_processing, operatingsystem, release)
				package_modify(Aqueduct, target_dir, var_dictionary)

				arches = []
				f = open(target_dir+'debian/control', 'r')
				control = f.read().split('\n')
				for line in control:
					if line.startswith('Architecture'):
						line = line.split(':', 1)[-1].replace(' ', '')
						if line == 'any':
							arches = conf.repos[operatingsystem][release]['arches']
							try:
								arches.remove('source')
							except ValueError:
								pass
						else:
							arches = line.split(',')
						break
				f.close()

				for arch in arches:
					print("Adding build for arch %s, os %s, release %s" % (arch, operatingsystem, release))
					builds.append({'arch':arch, 'os':operatingsystem, 'release':release, 'sourcedir':target_dir})

		jobid = db.add_tasks(builds)
		for b in range(0,len(builds)):
			builds[b]['jobid'] = jobid
		builder_interface.queue_tasks(builds)
				

	else:
		print("Unrecognized aqueduct version: " + Aqueduct['version'])
