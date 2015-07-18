# sftp.py - dput method for sftp transport
#
# @author Cody A.W. Somerville <cody.somerville@canonical.com>
# @company Canonical Ltd.
# @date 07 November 2008
#

import os, sys



class FileWithProgress:
  # FileWithProgress(f, args)
  # mimics a file (passed as f, an open file), but with progress.
  # args: ptype = 1,2 is the type ("|/-\" or numeric), default 0 (no progress)
  #       progressf = file to output progress to (default sys.stdout)
  #       size = size of file (or -1, the default, to ignore)
  #              for numeric output
  #       step = stepsize (default 1024)
  def __init__ (self, f, ptype=0, progressf=sys.stdout, size=-1, step=1024):
    self.f = f
    self.count = 0
    self.lastupdate = 0
    self.ptype = ptype
    self.ppos = 0
    self.progresschars = ['|','/','-','\\']
    self.progressf = progressf
    self.size=size
    self.step=step
    self.closed=0
  def __getattr__(self, name):
    return getattr(self.f, name)
  def read(self, size=-1):
    a = self.f.read(size)
    self.count = self.count + len(a)
    if (self.count-self.lastupdate)>1024:
      if self.ptype == 1:
        self.ppos = (self.ppos+1)%len(self.progresschars)
        self.progressf.write((self.lastupdate!=0)*"\b"+
                             self.progresschars[self.ppos])
        self.progressf.flush()
        self.lastupdate = self.count
      elif self.ptype == 2:
        s = str(self.count/self.step)+"k"
        if self.size >= 0:
          s += '/'+str((self.size+self.step-1)/self.step)+'k'
        s += min(self.ppos-len(s),0)*' '
        self.progressf.write(self.ppos*"\b"+s)
        self.progressf.flush()
        self.ppos = len(s)
    return a
  def close(self):
    if not self.closed:
      self.f.close()
      self.closed = 1
      if self.ptype==1:
        if self.lastupdate:
          self.progressf.write("\b \b")
          self.progressf.flush()
      elif self.ptype==2:
        self.progressf.write(self.ppos*"\b"+self.ppos*" "+self.ppos*"\b")
        self.progressf.flush()
  def __del__(self):
    self.close()



def upload(fqdn, login, incoming, files, debug, compress, progress=0):
    try:
        import bzrlib.transport
    except Exception, e:
        print "E: bzrlib must be installed to use sftp transport."
        sys.exit(1)

    if not login or login == '*':
        login = os.getenv("USER")

    if not incoming.endswith("/"):
        incoming = "%s/" % incoming

    try:
        t = bzrlib.transport.get_transport("sftp://%s@%s/%s" % (login, fqdn, incoming))
    except Exception, e:
        print "%s\nE: Error connecting to remote host." % e
        sys.exit(1)

    for f in files:
        baseFilename = f.split("/")[len(f.split("/"))-1]
        sys.stdout.write("  %s: " % baseFilename)
        sys.stdout.flush()
        try:
            fileobj = open(f, 'rb')

            if progress:
                try:
                    size = os.stat(f).st_size
                except:
                    size = -1
                    if debug:
                        print "D: Determining size of file '%s' failed" % f

                fileobj = FileWithProgress(fileobj, ptype=progress,
                                                      progressf=sys.stdout,
                                                      size=size)

            t.put_file(baseFilename, fileobj)
            fileobj.close()
        except Exception, e:
            print "\n%s\nE: Error uploading file." % e
            sys.exit(1)
        print "done."
