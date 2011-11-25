import subprocess
from tempfile import TemporaryFile
import simplejson as json
from fabric.api import env
from fabric.api import local


REMOTE_REPO = 'chm-cube'

try: from local_fabfile import *
except: pass


def to_cube(name):
    local('gzcat data/apache-logs/access.log-%(name)s.gz | '
          'python logpump.py chm-eu/%(name)s' % {'name': name})


_mongo_drop_js = """\
["chm_eu"].forEach(function(type) {
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


def run(cmd):
    return subprocess.check_output(['ssh', env['host_string'], cmd])


def deploy_logtail():
    run("git init '%s'" % REMOTE_REPO)

    git_remote = "%s:%s" % (env['host_string'], REMOTE_REPO)
    local("git push -f '%s' master:incoming" % git_remote)
    run("cd '%s'; git reset incoming --hard" % REMOTE_REPO)

    sandbox = REMOTE_REPO + '/sandbox'
    run("test -d '%(sandbox)s' || virtualenv --no-site-packages '%(sandbox)s'" %
        {'sandbox': sandbox})
    run("echo '*' > '%s/.gitignore'" % sandbox)

    run("cd '%s'; sandbox/bin/pip install -r requirements.txt" % REMOTE_REPO)


def logtail():
    last_id = None

    while True:
        print 'requesting records with last_id=%r' % last_id
        last_id_str = '' if last_id is None else "'%s'" % last_id
        events_json = run("%(repo)s/sandbox/bin/python "
                          "%(repo)s/logtail.py "
                          "%(logdir)s/access.log %(last_id)s" % {
                              'repo': REMOTE_REPO,
                              'logdir': '/var/local/www-logs/apache/',
                              'last_id': last_id_str,
                          })

        with TemporaryFile() as tmp:
            tmp.write(events_json)
            tmp.seek(0)
            state_json = subprocess.check_output(
                ['python', 'import_logtail.py'], stdin=tmp)

        state = json.loads(state_json)
        if not state['get_more']:
            print 'last_id=%r' % last_id
            break
        last_id = state['last_id']
