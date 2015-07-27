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
	address VARCHAR(255) NOT NULL,
	fingerprint VARCHAR(32) NOT NULL,
	arch VARCHAR(8) NOT NULL,
	os VARCHAR(16) NOT NULL,
	PRIMARY KEY(address),
	UNIQUE(fingerprint)
);
""",
"""
CREATE TABLE IF NOT EXISTS builder_releases (
	builderid VARCHAR(255),
	releasename VARCHAR(16) NOT NULL,
	PRIMARY KEY(builderid, releasename),
	FOREIGN KEY(builderid) REFERENCES builders(address)
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
	os VARCHAR(16) NOT NULL,
	releasename VARCHAR(16) NOT NULL,
	taskstatus ENUM('unassigned', 'assigned', 'built', 'failed') DEFAULT 'unassigned' NOT NULL,
	assignee VARCHAR(255),
	sourcedir VARCHAR(255) NOT NULL,
	resultdir VARCHAR(255),
	PRIMARY KEY(jobid, os, releasename),
	FOREIGN KEY(jobid) REFERENCES jobs(jobid),
	FOREIGN KEY(assignee) REFERENCES builders(address),
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



_ensure_tables_exist() #Runs on module load.
