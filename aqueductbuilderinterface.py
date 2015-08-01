#!/usr/bin/python3

import libaqueduct as lib



def submit(url, dirpath, postdata):
	lib.targz(dirpath, 'temp.tar.gz')
	lib.upload(url, 'temp.tar.gz', postdata)
