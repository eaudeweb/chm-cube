import os
import sys
from datetime import datetime, timedelta
import simplejson as json

"""
read apache log file incrementally
"""


def main():
    from apache_log import cube_events

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
    main()
