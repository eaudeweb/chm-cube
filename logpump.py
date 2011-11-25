import sys
import urllib
import logging
import time
import simplejson as json
from apache_log import parse_log


log = logging.getLogger('logpump')
log.setLevel(logging.DEBUG)


def pump_to_cube(entry_list, url, logfile_name):
    BATCH_SIZE = 10000

    def send_batch():
        result = None
        try:
            f = urllib.urlopen(url, json.dumps(batch))
            result = f.read()
            f.close()
            assert json.loads(result) == {'status': 200}

        except:
            log.exception("failed sending batch, result is %r", result)
            raise

        else:
            log.debug("sent batch of %d", len(batch))
            batch[:] = []

    batch = []
    for n, entry in enumerate(entry_list):
        event = {
            'id': "%s/%d" % (logfile_name, n),
            'type': 'chm_eu_request',
            'time': entry.datetime.isoformat(),
            'data': {
                'duration_ms': entry.duration * 10**3,
                'path': entry.url,
                'status': entry.status,
                'size': entry.size,
                'vhost': entry.vhost,
            },
        }
        batch.append(event)

        if len(batch) >= BATCH_SIZE:
            send_batch()

    if batch:
        send_batch()


def main():
    url = "http://localhost:1080/1.0/event/put"
    pump_to_cube(parse_log(sys.stdin), url, sys.argv[1])


if __name__ == '__main__':
    logging.basicConfig(loglevel=logging.DEBUG)
    main()
