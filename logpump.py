import sys
import urllib
import logging
import time


def main():
    from apache_log import cube_events
    from to_cube import CubeUploader
    cube = CubeUploader("http://localhost:1080/1.0/event/put")
    log_file_name = sys.argv[1]
    for n, event in enumerate(cube_events(sys.stdin)):
        event['id'] = "%s/%d" % (log_file_name, n)
        cube.add(event)
    cube.flush()


if __name__ == '__main__':
    logging.basicConfig(loglevel=logging.DEBUG)
    main()
