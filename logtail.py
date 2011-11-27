import os
import sys
from datetime import datetime, timedelta
import time
import logging
import simplejson as json
from apache_log import parse_log_line, cube_event_for_entry

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

    def log_lines(self, skip_bytes):
        log_file_path = os.path.join(self.log_dir, self.log_basename)
        log_file = open(log_file_path, 'rb')
        log_file.seek(skip_bytes)

        buf = ''
        prev_newline = 0
        bytes_count = skip_bytes

        while True:
            buf += log_file.read(BUFFER_SIZE)
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
                    prev_newline = 0
                    break

                line = buf[prev_newline:newline]
                yield line, bytes_count
                bytes_count += newline - prev_newline
                prev_newline = newline

    def determine_log_file_name(self):
        current_filename = os.path.join(self.log_dir, self.log_basename)
        old_filename = current_filename + '.1'

        old_mtime = datetime.fromtimestamp(os.stat(old_filename).st_mtime)
        old_year, old_week, old_weekday = old_mtime.isocalendar()
        assert old_weekday == 7
        year, week, n_weekday = (old_mtime + timedelta(days=7)).isocalendar()
        assert n_weekday == 7

        return 'chm-eu/%d-%d' % (year, week)

    def stream_log_entries(self, out_file, skip):
        log_file_name = self.determine_log_file_name()

        skip_bytes, skip_entries = map(int, skip.split('/'))

        for n, (line, bytes_count) in enumerate(self.log_lines(skip_bytes),
                                                skip_entries):
            entry = parse_log_line(line)
            event = cube_event_for_entry(entry)
            event['id'] = "%s/%d" % (log_file_name, n)
            event['_skip'] = '%d/%d' % (bytes_count + len(line), n + 1)
            json.dump(event, out_file)
            out_file.write('\n')


def main():
    log_dir, log_basename, skip = sys.argv[1:4]
    log_reader = LogReader(log_dir, log_basename)
    log_reader.stream_log_entries(sys.stdout, skip)


if __name__ == '__main__':
    main()
