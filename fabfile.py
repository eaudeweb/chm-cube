from fabric.api import local


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
    import subprocess
    from tempfile import TemporaryFile

    with TemporaryFile() as tmp:
        tmp.write(code)
        tmp.seek(0)
        subprocess.check_call(['mongo', 'cube_development'], stdin=tmp)
        tmp.close()


def mongo_drop():
    _mongo(_mongo_drop_js)


def mongo_schema():
    _mongo(_mongo_schema_js)
