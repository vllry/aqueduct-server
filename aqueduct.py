from os import popen



class config:
	"""Load and process the config files"""


	def __init__(self, config_file_path = "/home/vallery/Desktop/conduit/aqueduct/etc/aqueduct/repositories.conf"):
		from os import listdir, path
		from configparser import ConfigParser

		attributes = [	'arch',
						'components',
						'distributions']
		self.repos = {}
		self.releases = {}

		rootconfig = ConfigParser()
		rootconfig.read(config_file_path)
		for os in rootconfig.sections():
			self.repos[os] = {
							'dev' : rootconfig[os]['dev'].replace(' ', '').split(','),
							'supported' : rootconfig[os]['supported'].replace(' ', '').split(','),
							'all' : rootconfig[os]['all'].replace(' ', '').split(','),
							}
			self.releases[os] = {}

			repoconfig = ConfigParser()
			repoconfig.read('/home/vallery/Desktop/conduit/aqueduct/etc/aqueduct/releases/'+os+'.conf')
			for release in repoconfig.sections():
				self.releases[os][release] = {}

				for attr in attributes:
					if attr in repoconfig[release]:
						self.releases[os][release][attr] = repoconfig[release][attr].replace(' ', '').replace('[RELEASE]', release).split(',')
					elif attr in rootconfig[os]:
						self.releases[os][release][attr] = rootconfig[os][attr].replace(' ', '').replace('[RELEASE]', release).split(',')
					else:
						print("uh oh, couldn't find " + attr)



class repo_deb:
	"""Manage the deb repositories"""


	def __init__(self, conf):
		self.config = conf
		self.repos = popen("aptly repo list -raw").read().split('\n')
		print(self.repos)


	def create(self, release, component):
		name = release + '-' + component
		if name not in self.repos:
			print(popen("aptly repo create --distribution=\"" + release + "\" --component=\"" + component + "\" " + name).read())
			self.repos.append(name)


	def add(self, repo, file):
		popen('aptly repo add ' + repo + ' ' + file)



def str_replace(s, values_original):
	"""Replaces all instances in s (regardless of case) of KEY with values[KEY]"""

	values = {}
	for key in values_original.keys(): #Add brackets around key values
		values['{'+key+'}'] = values_original[key]

	s_lower = s.lower()
	for key in values.keys():
		pos = s_lower.find(key)
		while pos != -1:
			old = s[pos:pos+len(key)]
			s = s.replace(old, values[key])
			s_lower = s.lower()
			pos = s_lower.find(key)
	return s
