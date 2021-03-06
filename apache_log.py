from __future__ import division
from collections import namedtuple
import re
from dateutil.parser import parse as parse_date


LogEntry = namedtuple('LogEntry', 'datetime duration method url vhost '
                                  'status size referrer useragent client')

log_line_pattern = re.compile(
    r'^(?P<client>\S+) \S+ \S+ '
    r'\[(?P<datetime>[^\]]+)\] '
    r'"(?P<method>\w+) (?P<url>\S*)( [^"]+)?" '
    r'(?P<status>\d+) '
    r'(?P<size>\S+) ' # number or "-"
    r'"(?P<referrer>([^"]|\\")*)" ' # referrer may contain '\"'
    r'"(?P<useragent>([^"]|\\")+)" ' # user agent may contain '\"'
    r'(?P<duration>\d+) '
    r'(?P<vhost>\S+)$')


def parse_log_line(line):
    match = log_line_pattern.match(line.strip())
    if match is None:
        return None
    str_size = match.group('size')
    size = (0 if str_size == '-' else int(str_size))
    rdt = match.group('datetime')
    return LogEntry(datetime=parse_date(rdt[:11] + ' ' + rdt[12:]),
                    duration=int(match.group('duration')) / 10**6,
                    method=match.group('method'),
                    url=match.group('url'),
                    vhost=match.group('vhost'),
                    status=int(match.group('status')),
                    size=size,
                    referrer=match.group('referrer'),
                    useragent=match.group('useragent'),
                    client=match.group('client'))


def parse_log(input_lines):
    for n, line in enumerate(input_lines):
        entry = parse_log_line(line)
        assert entry is not None, "line %d does not match: %r" % (n, line)
        yield entry


def cube_event_for_entry(entry):
    return {
        'type': 'chm_eu',
        'time': entry.datetime.isoformat(),
        'data': {
            'client': entry.client,
            'duration_ms': entry.duration * 10**3,
            'method': entry.method,
            'path': entry.url,
            'referrer': entry.referrer,
            'status': entry.status,
            'size': entry.size,
            'useragent': entry.useragent,
            'vhost': entry.vhost,
        },
    }


def cube_events(log_file):
    for entry in parse_log(log_file):
        yield cube_event_for_entry(entry)
