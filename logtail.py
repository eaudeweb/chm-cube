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

    last_id = sys.argv[2] if len(sys.argv) > 2 else None

    log_file = open(current_filename)
    batch_size = 10000
    count = 0
    for n, event in enumerate(cube_events(log_file)):
        event_id = "%s/%d" % (log_file_name, n)
        if last_id is not None:
            if event_id == last_id:
                last_id = None
            continue
        event['id'] = event_id
        print json.dumps(event)
        count += 1
        if count >= batch_size:
            break
    else:
        print json.dumps(None)
    log_file.close()


if __name__ == '__main__':
    main()
