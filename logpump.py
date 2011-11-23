from __future__ import division
import sys
import urllib
from collections import namedtuple
import re
import simplejson as json
from dateutil.parser import parse as parse_date


LogEntry = namedtuple('LogEntry', 'datetime duration method url vhost '
                                  'status size useragent client')

def parse_log(input_lines):
    pattern = re.compile(
        r'^(?P<client>\S+) \S+ \S+ '
        r'\[(?P<datetime>[^\]]+)\] '
        r'"(?P<method>\w+) (?P<url>\S*)( [^"]+)?" '
        r'(?P<status>\d+) '
        r'(?P<size>\S+) ' # number or "-"
        r'"(?P<referrer>[^"]*)" '
        r'"(?P<useragent>([^"]|\")+)" ' # referrer may contain '\"'
        r'(?P<duration>\d+) '
        r'(?P<vhost>\S+)$')

    for n, line in enumerate(input_lines):
        match = pattern.match(line.strip())
        assert match is not None, "line %d does not match: %r" % (n, line)
        str_size = match.group('size')
        size = (0 if str_size == '-' else int(str_size))
        rdt = match.group('datetime')
        yield LogEntry(datetime=parse_date(rdt[:11] + ' ' + rdt[12:]),
                       duration=int(match.group('duration')) / 10**6,
                       method=match.group('method'),
                       url=match.group('url'),
                       vhost=match.group('vhost'),
                       status=match.group('status'),
                       size=size,
                       useragent=match.group('useragent'),
                       client=match.group('client'))


def pump_to_cube(entry_list, url):
    BATCH_SIZE = 1000
    def send_batch():
        f = urllib.urlopen(url, json.dumps(batch))
        result = f.read()
        f.close()
        assert json.loads(result) == {'status': 200}
        batch[:] = []

    batch = []
    for entry in entry_list:
        event = {
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
    pump_to_cube(parse_log(sys.stdin), url)


if __name__ == '__main__':
    main()
