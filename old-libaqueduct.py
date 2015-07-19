from os import popen



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
