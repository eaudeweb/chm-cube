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

    log_file = open(current_filename)
    for n, event in enumerate(cube_events(log_file)):
        if n > 10:
            break
        event['id'] = "%s/%d" % (log_file_name, n)
        print json.dumps(event)
    log_file.close()


if __name__ == '__main__':
    main()
