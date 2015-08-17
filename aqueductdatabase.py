from json import load
import pymysql as mysql



def dict_from_tup(keys, values):
	d = {}
	if not values:
		return {}
	elif len(keys) != len(values):
		print('WARNING: aqueductdatabase.dict_from_tup() given an unequal number of items and keys')

	for i in range(0,len(keys)):
		d[keys[i]] = values[i]
	return d



def dict_from_tup_list(keys, values_list):
	l = []
	for values in values_list:
		l.append(dict_from_tup(keys, values))

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
	online BIT NOT NULL DEFAULT 0,
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
	taskstatus ENUM('unassigned', 'assigned', 'built', 'failed', 'cancelled') DEFAULT 'unassigned' NOT NULL,
	resultdir VARCHAR(255),
	PRIMARY KEY(jobid, build_os, build_release, build_arch),
	FOREIGN KEY(jobid) REFERENCES jobs(jobid)
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
	unassign_after INT NOT NULL,
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



def get_builder_attribute(builder_address, builder_fingerprint, attr):
	con = _connect()
	cur = con.cursor()
	cur.execute("""
SELECT %s
FROM builders
WHERE address='%s' AND fingerprint='%s'
""" % (attr, builder_address, builder_fingerprint))
	return cur.fetchone()[0]



def set_builder_attribute(builder_address, builder_fingerprint, attr, value):
	con = _connect()
	cur = con.cursor()
	cur.execute("""
UPDATE builders
SET %s='%s'
WHERE address='%s' AND fingerprint='%s'
""" % (attr, value, builder_address, builder_fingerprint))
	con.commit()



def add_builder_release(builder_address, builder_fingerprint, release):
	con = _connect()
	cur = con.cursor()
	cur.execute("INSERT INTO builder_releases(builder_address, builder_fingerprint, releasename) VALUES('%s', '%s', '%s')" % (builder_address, builder_fingerprint, release))
	con.commit()



def remove_builder_release(builder_address, builder_fingerprint, release):
	con = _connect()
	cur = con.cursor()
	cur.execute("""
DELETE
FROM builder_releases
WHERE builder_address='%s' AND builder_fingerprint='%s' AND releasename='%s'
""" % (builder_address, builder_fingerprint, release))
	con.commit()



def get_builder_releases(builder_address, builder_fingerprint):
	con = _connect()
	cur = con.cursor()
	cur.execute("""
SELECT releasename
FROM builder_releases
WHERE builder_address='%s' AND builder_fingerprint='%s'
""" % (builder_address, builder_fingerprint))
	l = []
	for release in cur.fetchall(): #Returns a tuple of 1-element tuples
		l.append(release[0])
	return l



def update_builder(address, fingerprint, label, arch, os, releases):
	con = _connect()
	cur = con.cursor()
	cur.execute("""
UPDATE builders
SET arch='%s', os='%s', label='%s'
WHERE address='%s' AND fingerprint='%s'
""" % (arch, os, label, address, fingerprint))
	con.commit()
	for release in releases:
		add_builder_release(address, fingerprint, release)



def mark_all_builders_offline():
	con = _connect()
	cur = con.cursor()
	cur.execute("UPDATE builders SET online=0")
	con.commit()



def mark_builder_online(address, fingerprint, state):
	con = _connect()
	cur = con.cursor()
	cur.execute("UPDATE builders SET online=%s WHERE address='%s' AND fingerprint='%s'" % (state, address, fingerprint))
	con.commit()



def is_builder_online(address, fingerprint):
	con = _connect()
	cur = con.cursor()
	cur.execute("""
SELECT online
FROM builders
WHERE address='%s' AND fingerprint='%s'
""" % (address, fingerprint))
	value = cur.fetchone()[0]
	if value == b'\x01':
		return True
	return False



def add_job():
	con = _connect()
	cur = con.cursor()
	cur.execute("INSERT INTO jobs(jobstatus) VALUES('processing')")
	cur.execute("SELECT LAST_INSERT_ID()") #TODO: investigate safer alternative
	con.commit()
	jobid = cur.fetchone()[0]
	return jobid



def add_tasks_to_job(tasks, jobid):
	con = _connect()
	cur = con.cursor()
	for target in tasks:
		cur.execute("INSERT INTO tasks(jobid, build_arch, build_release, build_os) VALUES('%s', '%s', '%s', '%s')" % (jobid, target['arch'], target['release'], target['os']))
	con.commit()



def task_done(jobid, arch, os, release):
	con = _connect()
	cur = con.cursor()
	cur.execute("UPDATE tasks SET taskstatus='built' WHERE jobid='%s' AND build_arch='%s' AND build_os='%s' AND build_release='%s'" % (jobid, arch, os, release))
	cur.execute("DELETE FROM assignments WHERE jobid='%s' AND build_arch='%s' AND build_os='%s' AND build_release='%s'" % (jobid, arch, os, release))
	con.commit()



def task_failed(jobid, arch, os, release):
	con = _connect()
	cur = con.cursor()
	cur.execute("UPDATE tasks SET taskstatus='cancelled' WHERE jobid='%s'" % (jobid))
	cur.execute("UPDATE tasks SET taskstatus='failed' WHERE jobid='%s' AND build_arch='%s' AND build_os='%s' AND build_release='%s'" % (jobid, arch, os, release))
	cur.execute("DELETE FROM assignments WHERE jobid='%s'" % (jobid))
	con.commit()



def get_unassigned_tasks():
	con = _connect()
	cur = con.cursor()
	cur.execute("SELECT jobid, build_arch, build_os, build_release FROM tasks WHERE taskstatus='unassigned'")
	return dict_from_tup_list(('jobid', 'arch', 'os', 'release'), cur.fetchall())



def assign_task(builder_address, builder_fingerprint, jobid, build_arch, build_os, build_release):
	con = _connect()
	cur = con.cursor()
	cur.execute("UPDATE tasks SET taskstatus='assigned' WHERE jobid='%s' AND build_arch='%s' AND build_os='%s' AND build_release='%s'" % (jobid, build_arch, build_os, build_release))
	cur.execute("INSERT INTO assignments(builder_address, builder_fingerprint, jobid, build_arch, build_os, build_release, unassign_after) VALUES('%s', '%s', '%s', '%s', '%s', '%s', UNIX_TIMESTAMP()+1800)" % (builder_address, builder_fingerprint, jobid, build_arch, build_os, build_release))
	con.commit()



def unassign_tasks_from_builder(builder_address, builder_fingerprint):
	con = _connect()
	cur = con.cursor()
	cur.execute("""
UPDATE tasks SET taskstatus='unassigned'
WHERE EXISTS 
	(SELECT 1
		FROM assignments AS a
		WHERE a.builder_address = '%s' AND a.builder_fingerprint = '%s'
			AND a.jobid = tasks.jobid AND a.build_arch = tasks.build_arch AND a.build_os = tasks.build_os AND a.build_release = tasks.build_release
	);
""" % (builder_address, builder_fingerprint))
	cur.execute("DELETE FROM assignments WHERE builder_address='%s' AND builder_fingerprint='%s'" % (builder_address, builder_fingerprint))
	con.commit()



def unassign_task_from_builder(builder_address, builder_fingerprint, jobid, arch, os, release):
	con = _connect()
	cur = con.cursor()
	cur.execute("""
UPDATE tasks SET taskstatus='unassigned'
WHERE jobid = '%s' AND build_arch = '%s' AND build_os = '%s' AND build_release = '%s' AND EXISTS 
	(SELECT 1
		FROM assignments AS a
		WHERE a.builder_address = '%s' AND a.builder_fingerprint = '%s'
			AND a.jobid = '%s' AND a.build_arch = '%s' AND a.build_os = '%s' AND a.build_release = '%s'
	);
""" % (jobid, arch, os, release, builder_address, builder_fingerprint, jobid, arch, os, release))

	cur.execute("""
DELETE FROM assignments
WHERE builder_address = '%s' AND builder_fingerprint = '%s' AND jobid = '%s' AND build_arch = '%s' AND build_os = '%s' AND build_release = '%s'
""" % (builder_address, builder_fingerprint, jobid, arch, os, release))
	con.commit()



def delete_old_assignments():
	con = _connect()
	cur = con.cursor()
	cur.execute("""
UPDATE tasks
SET taskstatus='unassigned'
WHERE EXISTS 
	(SELECT 1
		FROM assignments AS a
		WHERE a.unassign_after < UNIX_TIMESTAMP()
	);
""")
	cur.execute("""
DELETE FROM assignments
WHERE unassign_after < UNIX_TIMESTAMP()
""")
	con.commit()



def get_tasks_assigned_to_builder(builder_address, builder_fingerprint):
	con = _connect()
	cur = con.cursor()
	cur.execute("""
SELECT jobid, build_arch, build_os, build_release
FROM tasks NATURAL JOIN assignments
WHERE builder_address='%s' AND builder_fingerprint='%s'
""" % (builder_address, builder_fingerprint))
	return dict_from_tup_list(('jobid', 'arch', 'os', 'release'), cur.fetchall())



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
WHERE %s os='%s' AND releasename='%s' AND online=1 AND
NOT EXISTS 
	(SELECT 1
		FROM assignments AS a
		WHERE a.builder_address = b.address AND a.builder_fingerprint = b.fingerprint
	);
""" % (arch_condition_string(arch), os, release))
	return dict_from_tup(('address', 'fingerprint'), cur.fetchone())



def get_free_builder(arch, os):
	con = _connect()
	cur = con.cursor()
	cur.execute("""
SELECT address, fingerprint
	FROM
	builders AS b
WHERE %s os='%s' AND online=1 AND
NOT EXISTS 
	(SELECT 1
		FROM assignments AS a
		WHERE a.builder_address = b.address AND a.builder_fingerprint = b.fingerprint
	);
""" % (arch_condition_string(arch), os))
	return dict_from_tup(('address', 'fingerprint'), cur.fetchone())



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
SELECT address, fingerprint
	FROM
	builders
""")
	return cur.fetchall()



_ensure_tables_exist() #Runs on module load.
