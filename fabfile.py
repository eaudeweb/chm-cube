import os
import subprocess
from tempfile import TemporaryFile
import simplejson as json
from fabric.api import env
from fabric.api import local


LOGTAIL_REMOTE_REPO = 'chm-cube'
LOGTAIL_STATE_PATH = None


try: from local_fabfile import *
except: pass


def to_cube(name):
    local('gzcat data/apache-logs/access.log-%(name)s.gz | '
          'python logpump.py chm-eu/%(name)s' % {'name': name})


_mongo_drop_js = """\
["chm_eu"].forEach(function(type) {
  var event = type + "_events", metric = type + "_metrics";
  db[event].drop();
  db[metric].drop();
});
"""


_mongo_schema_js = """\
["chm_eu"].forEach(function(type) {
  var MB = 1024*1024;
  var event = type + "_events", metric = type + "_metrics";
  db.createCollection(event);
  db[event].ensureIndex({t: 1});
  db.createCollection(metric, {capped: true, size: 10*MB, autoIndexId: false});
  db[metric].ensureIndex({e: 1, l: 1, t: 1, g: 1}, {unique: true});
  db[metric].ensureIndex({i: 1, e: 1, l: 1, t: 1});
  db[metric].ensureIndex({i: 1, l: 1, t: 1});
});
"""


def _mongo(code):
    with TemporaryFile() as tmp:
        tmp.write(code)
        tmp.seek(0)
        subprocess.check_call(['mongo', 'cube_development'], stdin=tmp)


def mongo_drop():
    _mongo(_mongo_drop_js)


def mongo_schema():
    _mongo(_mongo_schema_js)


def manual_run(cmd):
    """ `run` script that uses the system's ssh instead of paramiko """
    return subprocess.check_output(['ssh', '-C', env['host_string'], cmd])


def deploy_logtail():
    run = manual_run # beacause we need tunneling

    run("git init '%s'" % LOGTAIL_REMOTE_REPO)

    git_remote = "%s:%s" % (env['host_string'], LOGTAIL_REMOTE_REPO)
    local("git push -f '%s' master:incoming" % git_remote)
    run("cd '%s'; git reset incoming --hard" % LOGTAIL_REMOTE_REPO)

    sandbox = LOGTAIL_REMOTE_REPO + '/sandbox'
    run("test -d '%(sandbox)s' || virtualenv --no-site-packages '%(sandbox)s'" %
        {'sandbox': sandbox})
    run("echo '*' > '%s/.gitignore'" % sandbox)

    run("cd '%s'; sandbox/bin/pip install -r logtail-requirements.txt" %
        LOGTAIL_REMOTE_REPO)


def _wait_keyboard_interrupt():
    import time
    while True:
        try:
            time.sleep(100)
        except KeyboardInterrupt:
            return


def logtail(skip=None):
    if skip is None:
        skip = ''
        if LOGTAIL_STATE_PATH is not None and os.path.isfile(LOGTAIL_STATE_PATH):
            with open(LOGTAIL_STATE_PATH, 'rb') as f:
                skip = f.read().strip()

    print 'requesting records with skip=%r' % skip
    cmd = ("%(repo)s/sandbox/bin/python "
           "%(repo)s/logtail.py "
           "%(logdir)s access.log '%(skip)s'") % {
               'repo': LOGTAIL_REMOTE_REPO,
               'logdir': '/var/local/www-logs/apache/',
               'skip': skip,
           }

    ssh_logtail = subprocess.Popen(['ssh', '-C', env['host_string'], cmd],
                                   stdout=subprocess.PIPE)
    import_logtail = subprocess.Popen(['python', 'import_logtail.py'],
                                      stdin=ssh_logtail.stdout,
                                      stdout=subprocess.PIPE)
    _wait_keyboard_interrupt()
    next_skip = import_logtail.communicate()[0]
    print next_skip,
    if LOGTAIL_STATE_PATH is not None:
        with open(LOGTAIL_STATE_PATH, 'wb') as f:
            f.write(next_skip)

    ssh_logtail.terminate()
    ssh_logtail.wait()
