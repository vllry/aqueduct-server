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
	address VARCHAR(255) NOT NULL,
	fingerprint VARCHAR(64) NOT NULL,
	pubkey TEXT NOT NULL,
	label VARCHAR(16) NOT NULL,
	arch VARCHAR(8) NOT NULL,
	os VARCHAR(16) NOT NULL,
	PRIMARY KEY(fingerprint, address),
	UNIQUE(fingerprint, pubkey)
);
""",
"""
CREATE TABLE IF NOT EXISTS builder_releases (
	builder_address VARCHAR(255) NOT NULL,
	builder_fingerprint VARCHAR(64) NOT NULL,
	releasename VARCHAR(16) NOT NULL,
	PRIMARY KEY(builder_address, builder_fingerprint, releasename),
	FOREIGN KEY(builder_address, builder_fingerprint) REFERENCES builders(address, fingerprint)
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
	build_os VARCHAR(16) NOT NULL,
	build_release VARCHAR(16) NOT NULL,
	build_arch VARCHAR(8) NOT NULL,
	taskstatus ENUM('unassigned', 'assigned', 'built', 'failed') DEFAULT 'unassigned' NOT NULL,
	builder_address VARCHAR(255),
	builder_fingerprint VARCHAR(64),
	sourcedir VARCHAR(255) NOT NULL,
	resultdir VARCHAR(255),
	PRIMARY KEY(jobid, build_os, build_release),
	FOREIGN KEY(jobid) REFERENCES jobs(jobid),
	FOREIGN KEY(builder_address, builder_fingerprint) REFERENCES builders(address, fingerprint),
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



def add_builder(label, address, fingerprint, pubkey, arch, os):
	con = _connect()
	cur = con.cursor()
	cur.execute("INSERT INTO builders(label, address, fingerprint, pubkey, arch, os) VALUES('%s', '%s', '%s', '%s', '%s', '%s')" % (label, address, fingerprint, pubkey, arch, os))
	con.commit()



def add_builder_release(builder_address, builder_fingerprint, release):
	con = _connect()
	cur = con.cursor()
	cur.execute("INSERT INTO builder_releases(builder_address, builder_fingerprint, releasename) VALUES('%s', '%s', '%s')" % (builder_address, builder_fingerprint, release))
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
