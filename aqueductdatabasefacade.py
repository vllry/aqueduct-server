#!/usr/bin/python

from json import load
import pymysql as mysql



def _connect():
	f = open('/home/vallery/Development/Aqueduct/aqueduct-server/etc/aqueduct-server/database.conf')
	conf = load(f)
	f.close()
	return mysql.connect(conf['host'], conf['user'], conf['password'], conf['database'])



def ensure_tables_exist():
	builders = """
CREATE TABLE IF NOT EXISTS builders (
	address VARCHAR(255) NOT NULL,
	fingerprint VARCHAR(32) NOT NULL,
	arch VARCHAR(8) NOT NULL,
	os VARCHAR(16) NOT NULL,
	PRIMARY KEY(address),
	UNIQUE(fingerprint)
);
"""
	builder_releases = """
CREATE TABLE IF NOT EXISTS builder_releases (
	builderid VARCHAR(255),
	releasename VARCHAR(16) NOT NULL,
	PRIMARY KEY(builderid, releasename),
	FOREIGN KEY(builderid) REFERENCES builders(address)
);
"""

	con = _connect()
	cur = con.cursor()
	cur.execute(builders)
	cur.execute(builder_releases)
	con.commit()






ensure_tables_exist()
