import sys
import logging
import threading
import time
import simplejson as json


FLUSH_INTERVAL = 10 # seconds


class FlushThread(threading.Thread):
    def __init__(self, cube, lock):
        super(FlushThread, self).__init__()
        self.cube = cube
        self.lock = lock
        self.stop = False

    def run(self):
        while not self.stop:
            with self.lock:
                self.cube.flush()
            time.sleep(FLUSH_INTERVAL)


def main():
    from to_cube import CubeUploader
    cube = CubeUploader("http://localhost:1080/1.0/event/put")
    flush_lock = threading.Lock()
    flush_thread = FlushThread(cube, flush_lock)
    flush_thread.start()

    skip = None
    try:
        while True:
            line = sys.stdin.readline()
            if not line:
                break
            event = json.loads(line)
            event['data']['logtail'] = True
            skip = event.pop('_skip')
            with flush_lock:
                cube.add(event)

    except KeyboardInterrupt:
        pass

    finally:
        print "Next log record at offset %r." % (skip,)

    flush_thread.stop = True
    flush_thread.join()
    cube.flush()


if __name__ == '__main__':
    logging.basicConfig(loglevel=logging.DEBUG)
    main()
