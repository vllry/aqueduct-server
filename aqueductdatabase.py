from json import load
import pymysql as mysql



def build_dict(items, keys):
	d = {}
	if not items:
		return {}
	elif len(items) != len(keys):
		print('WARNING: aqueductdatabase.build_dict() given an unequal number of items and keys')
	for i in range(0,len(keys)):
		d[keys[i]] = items[i]
	return d



def build_dict_list(items_list, keys):
	l = []
	if not items_list:
		return []
	for items in items_list:
		d = {}
		if len(items) != len(keys):
			print('WARNING: aqueductdatabase.build_dict_list() given an unequal number of items and keys')
		for i in range(0,len(keys)):
			d[keys[i]] = items[i]
		l.append(d)
	return l



def _connect():
	f = open('/home/vallery/Development/Aqueduct/aqueduct-server/etc/aqueduct-server/database.conf')
	conf = load(f)
	f.close()
	return mysql.connect(conf['host'], conf['user'], conf['password'], conf['database'])



#Function to create/modify tables, to create the expected environment
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
	KEY(fingerprint),
	KEY(address)
);
""",
"""
CREATE TABLE IF NOT EXISTS builder_releases (
	builder_address VARCHAR(255) NOT NULL,
	builder_fingerprint VARCHAR(64) NOT NULL,
	releasename VARCHAR(16) NOT NULL,
	PRIMARY KEY(builder_address, builder_fingerprint, releasename),
	FOREIGN KEY builder_id_fkeys (builder_address, builder_fingerprint) REFERENCES builders(address, fingerprint)
) ENGINE=InnoDB;
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
	PRIMARY KEY(jobid, build_os, build_release, build_arch),
	FOREIGN KEY(jobid) REFERENCES jobs(jobid),
	FOREIGN KEY(builder_address, builder_fingerprint) REFERENCES builders(address, fingerprint)
) ENGINE=InnoDB;
""",
"""
CREATE TABLE IF NOT EXISTS assignments (
	jobid INT NOT NULL,
	build_arch VARCHAR(8) NOT NULL,
	build_os VARCHAR(16) NOT NULL,
	build_release VARCHAR(16) NOT NULL,
	builder_address VARCHAR(255),
	builder_fingerprint VARCHAR(64),
	PRIMARY KEY(jobid, build_os, build_release, build_arch),
	FOREIGN KEY(jobid) REFERENCES jobs(jobid),
	FOREIGN KEY(builder_address, builder_fingerprint) REFERENCES builders(address, fingerprint)
) ENGINE=InnoDB;
"""
]

	con = _connect()
	cur = con.cursor()
	for table in tables:
		#print('Running ' + table[:50] + '...')
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
	jobid = cur.fetchone()[0]
	for target in tasks:
		cur.execute("INSERT INTO tasks(jobid, build_arch, build_release, build_os, sourcedir) VALUES('%s', '%s', '%s', '%s', '%s')" % (jobid, target['arch'], target['release'], target['os'], target['sourcedir']))
	con.commit()
	return jobid



def get_unassigned_tasks():
	con = _connect()
	cur = con.cursor()
	cur.execute("SELECT jobid, build_arch, build_os, build_release, sourcedir FROM tasks WHERE taskstatus='unassigned'")
	return cur.fetchall()



def assign_task(builder_address, builder_fingerprint, jobid, build_arch, build_os, build_release):
	con = _connect()
	cur = con.cursor()
	cur.execute("UPDATE tasks SET taskstatus='assigned' WHERE jobid='%s' AND build_arch='%s' AND build_os='%s' AND build_release='%s'" % (jobid, build_arch, build_os, build_release))
	cur.execute("INSERT INTO assignments(builder_address, builder_fingerprint, jobid, build_arch, build_os, build_release) VALUES('%s', '%s', '%s', '%s', '%s', '%s')" % (builder_address, builder_fingerprint, jobid, build_arch, build_os, build_release))
	con.commit()



def arch_condition_string(arch):
	if arch == 'all':
		return ''
	else:
		return "arch='%s' AND" % arch



def get_free_builder_supporting_release(arch, os, release):
	con = _connect()
	cur = con.cursor()
	cur.execute("""
SELECT address, fingerprint
	FROM
	builders AS b JOIN builder_releases ON b.address = builder_releases.builder_address AND b.fingerprint = builder_releases.builder_fingerprint
WHERE %s os='%s' AND releasename='%s' AND
NOT EXISTS 
	(SELECT 1
		FROM assignments AS a
		WHERE a.builder_address = b.address AND a.builder_fingerprint = b.fingerprint
	);
""" % (arch_condition_string(arch), os, release))
	return build_dict(cur.fetchone(), ('address', 'fingerprint'))



def get_free_builder(arch, os):
	con = _connect()
	cur = con.cursor()
	cur.execute("""
SELECT address, fingerprint
	FROM
	builders AS b
WHERE %s os='%s' AND
NOT EXISTS 
	(SELECT 1
		FROM assignments AS a
		WHERE a.builder_address = b.address AND a.builder_fingerprint = b.fingerprint
	);
""" % (arch_condition_string(arch), os))
	return build_dict(cur.fetchone(), ('address', 'fingerprint'))



def get_builder_supporting_release(arch, os, release):
	con = _connect()
	cur = con.cursor()
	cur.execute("""
SELECT address, fingerprint
	FROM
	builders JOIN builder_releases ON builders.address = builder_releases.builder_address AND builders.fingerprint = builder_releases.builder_fingerprint
WHERE %s os='%s' AND releasename='%s'
""" % (arch_condition_string(arch), os, release))
	return cur.fetchone()



def get_builder(arch, os):
	con = _connect()
	cur = con.cursor()
	cur.execute("""
SELECT address, fingerprint
	FROM
	builders
WHERE %s os='%s'
""" % (arch_condition_string(arch), os))
	return cur.fetchone()


def get_all_builders():
	con = _connect()
	cur = con.cursor()
	cur.execute("""
SELECT address, fingerprint, label, arch, os
	FROM
	builders
""")
	return cur.fetchall()



_ensure_tables_exist() #Runs on module load.
