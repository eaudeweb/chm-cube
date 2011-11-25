import sys
import urllib
import logging
import time
import simplejson as json
from apache_log import parse_log


def pump_to_cube(entry_list, cube, logfile_name):
    for n, entry in enumerate(entry_list):
        cube.add({
            'id': "%s/%d" % (logfile_name, n),
            'type': 'chm_eu',
            'time': entry.datetime.isoformat(),
            'data': {
                'duration_ms': entry.duration * 10**3,
                'path': entry.url,
                'status': entry.status,
                'size': entry.size,
                'vhost': entry.vhost,
            },
        })


def main():
    from to_cube import CubeUploader
    cube = CubeUploader("http://localhost:1080/1.0/event/put")
    pump_to_cube(parse_log(sys.stdin), cube, sys.argv[1])
    cube.flush()


if __name__ == '__main__':
    logging.basicConfig(loglevel=logging.DEBUG)
    main()
