#!/usr/bin/python

from json import load
import pymysql as mysql



def _connect():
	f = open('/home/vallery/Development/Aqueduct/aqueduct-server/etc/aqueduct-server/database.conf')
	conf = load(f)
	f.close()
	return mysql.connect(conf['host'], conf['user'], conf['password'], conf['database'])



#Function to create/modify tables to create the expected environment
def _ensure_tables_exist():
	tables = [
"""
CREATE TABLE IF NOT EXISTS builders (
	builderid VARCHAR(16) NOT NULL,
	address VARCHAR(255) NOT NULL,
	fingerprint VARCHAR(32) NOT NULL,
	pubkey TEXT NOT NULL,
	arch VARCHAR(8) NOT NULL,
	os VARCHAR(16) NOT NULL,
	PRIMARY KEY(builderid),
	UNIQUE(address),
	UNIQUE(pubkey)
);
""",
"""
CREATE TABLE IF NOT EXISTS builder_releases (
	builderid VARCHAR(16),
	releasename VARCHAR(16) NOT NULL,
	PRIMARY KEY(builderid, releasename),
	FOREIGN KEY(builderid) REFERENCES builders(builderid)
);
""",
"""
CREATE TABLE IF NOT EXISTS jobs (
	jobid INT NOT NULL AUTO_INCREMENT,
	jobstatus ENUM('processing', 'complete', 'failed') DEFAULT 'processing' NOT NULL,
	PRIMARY KEY(jobid)
);
""",
"""
CREATE TABLE IF NOT EXISTS tasks (
	jobid INT NOT NULL,
	buildos VARCHAR(16) NOT NULL,
	buildrelease VARCHAR(16) NOT NULL,
	buildarch VARCHAR(8) NOT NULL,
	taskstatus ENUM('unassigned', 'assigned', 'built', 'failed') DEFAULT 'unassigned' NOT NULL,
	builderid VARCHAR(16),
	sourcedir VARCHAR(255) NOT NULL,
	resultdir VARCHAR(255),
	PRIMARY KEY(jobid, buildos, buildrelease),
	FOREIGN KEY(jobid) REFERENCES jobs(jobid),
	FOREIGN KEY(builderid) REFERENCES builders(builderid),
	UNIQUE(sourcedir),
	UNIQUE(resultdir)
);
"""
]

	con = _connect()
	cur = con.cursor()
	for table in tables:
		cur.execute(table)
	con.commit()



def add_builder(builderid, address, fingerprint, pubkey, arch, os):
	con = _connect()
	cur = con.cursor()
	cur.execute("INSERT INTO builders(builderid, address, fingerprint, pubkey, arch, os) VALUES('%s', '%s', '%s', '%s', '%s')" % (builderid, address, fingerprint, pubkey, arch, os))
	con.commit()



def add_builder_release(builderid, release):
	con = _connect()
	cur = con.cursor()
	cur.execute("INSERT INTO builder_releases(builderid, releasename) VALUES('%s', '%s')" % (builderid, release))
	con.commit()



def add_tasks(tasks):
	con = _connect()
	cur = con.cursor()
	cur.execute("INSERT INTO jobs(jobstatus) VALUES('processing')")
	cur.execute("SELECT LAST_INSERT_ID()") #TODO: investigate safer alternative
	jobid = cur.fetchone()
	for target in tasks:
		cur.execute("INSERT INTO tasks(jobid, buildarch, buildrelease, buildos, sourcedir) VALUES(%s, '%s', '%s', '%s', '%s')" % (jobid, target['arch'], target['release', target['os'], target['sourcedir']))
	con.commit()



def get_free_builder_supporting_release(arch, os, release):
	con = _connect()
	cur = con.cursor()
	cur.execute("""
SELECT *
	FROM
	builders JOIN builder_releases ON builders.builderid = builder_releases.builderid
WHERE arch='%s' AND os='%s' AND releasename='%s' AND
builderid NOT IN
(SELECT builderid FROM tasks WHERE taskstatus='assigned')
""" % (arch, os, release))
	return cur.fetchone()



def get_free_builder(arch, os):
	con = _connect()
	cur = con.cursor()
	cur.execute("""
SELECT *
	FROM
	builders
WHERE arch='%s' AND os='%s' AND
builderid NOT IN
(SELECT builderid FROM tasks WHERE taskstatus='assigned')
""" % (arch, os, release))
	return cur.fetchone()



def get_builder_supporting_release(arch, os, release):
	con = _connect()
	cur = con.cursor()
	cur.execute("""
SELECT *
	FROM
	builders JOIN builder_releases ON builders.builderid = builder_releases.builderid
WHERE arch='%s' AND os='%s' AND releasename='%s'
""" % (arch, os, release))
	return cur.fetchone()



def get_builder(arch, os):
	con = _connect()
	cur = con.cursor()
	cur.execute("""
SELECT *
	FROM
	builders
WHERE arch='%s' AND os='%s'
""" % (arch, os, release))
	return cur.fetchone()



_ensure_tables_exist() #Runs on module load.
