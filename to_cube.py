import urllib
import simplejson as json
import logging
import pytz
from datetime import datetime


log = logging.getLogger('to_cube')
log.setLevel(logging.DEBUG)


class CubeUploader(object):

    def __init__(self, url, buffer_size=10000):
        self.url = url
        self.records = []
        self.buffer_size = buffer_size

    def add(self, record):
        self.records.append(record)
        if len(self.records) >= self.buffer_size:
            self.flush()

    def flush(self):
        result = None
        try:
            f = urllib.urlopen(self.url, json.dumps(self.records))
            result = f.read()
            f.close()
            assert json.loads(result) == {'status': 200}

        except:
            log.exception("failed sending batch, result is %r", result)
            raise

        else:
            log.debug("sent batch of %d", len(self.records))
            self.records[:] = []
