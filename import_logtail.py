import sys
import logging
import threading
import time
import simplejson as json


FLUSH_INTERVAL = 10 # seconds


def FlushThread(threading.Thread):
    def __init__(self, cube, lock):
        super(self, this).__init__()
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

    for line in sys.stdin:
        event = json.loads(line)
        event['data']['logtail'] = True
        with flush_lock:
            cube.add(event)

    flush_thread.stop = True
    flush_thread.join()
    cube.flush()


if __name__ == '__main__':
    logging.basicConfig(loglevel=logging.DEBUG)
    main()
