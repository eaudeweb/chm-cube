import os
import sys
from datetime import datetime, timedelta
import time
import logging
import simplejson as json

"""
read apache log file incrementally
"""


BUFFER_SIZE = 2**16 # 64KB
SLEEP_TIME = 30 # seconds

log = logging.getLogger('logtail')
log.setLevel(logging.DEBUG)


class LogReader(object):

    def __init__(self, log_dir, log_basename):
        self.log_dir = log_dir
        self.log_basename = log_basename

    def stream_log_entries(self):
        log_file_path = os.path.join(self.log_dir, self.log_basename)
        log_file = open(log_file_path, 'rb')

        buf = ''
        prev_newline = 0

        while True:
            buf += log_file.read(BUFFER_SIZE)
            if buf:
                log.debug("buffer is now %r" % buf)
            if not buf:
                # end-of-file: see if logging happens in another file
                # TODO check for another file

                time.sleep(SLEEP_TIME)
                log_file.seek(log_file.tell())
                continue

            while True:
                newline = buf.find('\n', prev_newline) + 1
                if newline < 1:
                    # roll the buffer, only keep unhandled line fragment
                    buf = buf[prev_newline:]
                    log.debug("rolled buffer to %d, fragment is %r",
                              prev_newline, buf)
                    prev_newline = 0
                    break

                log.debug("current line from %d to %d", prev_newline, newline)
                line = buf[prev_newline:newline]
                self.handle(line)
                prev_newline = newline

    def handle(self, line):
        log.debug("handling line %r", line)


def main():
    from apache_log import cube_events

    log_dir, log_basename = sys.argv[1:3]

    log_reader = LogReader(log_dir, log_basename)
    log_reader.stream_log_entries()
    return





    current_filename = sys.argv[1]
    old_filename = current_filename + '.1'

    old_mtime = datetime.fromtimestamp(os.stat(old_filename).st_mtime)
    old_year, old_week, old_weekday = old_mtime.isocalendar()
    assert old_weekday == 7
    year, week, n_weekday = (old_mtime + timedelta(days=7)).isocalendar()
    assert n_weekday == 7

    log_file_name = 'chm-eu/%d-%d' % (year, week)

    bytes_offset = int(sys.argv[2]) if len(sys.argv) > 2 else 0

    log_file = open(current_filename)
    log_file.seek(bytes_offset)

    log_file_state = {'bytes': bytes_offset}
    def log_file_lines():
        for line in log_file:
            log_file_state['bytes'] += len(line)
            yield line

    batch_size = 10000
    count = 0
    for n, event in enumerate(cube_events(log_file_lines())):
        event['id'] = "%s/%d" % (log_file_name, n)
        print json.dumps(event)
        count += 1
        if count >= batch_size:
            has_more = True
            break
    else:
        has_more = False

    log_file.close()

    print json.dumps({
        'logtail_end': True,
        'logtail_has_more': has_more,
        'logtail_bytes_offset': log_file_state['bytes'],
    })


if __name__ == '__main__':
    logging.basicConfig(loglevel=logging.DEBUG)
    BUFFER_SIZE = 46
    SLEEP_TIME = 1
    main()
