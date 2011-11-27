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

    bytes_offset = 0

    for line in sys.stdin:
        event = json.loads(line)
        if 'logtail_end' in event:
            get_more = event['logtail_has_more']
            bytes_offset = event['logtail_bytes_offset']
            break

        event['data']['logtail'] = True
        with flush_lock:
            cube.add(event)

    else:
        raise ValueError("json stream ended unexpectedly")

    flush_thread.stop = True
    flush_thread.join()
    cube.flush()

    print json.dumps({
        'bytes_offset': bytes_offset,
        'get_more': get_more,
    })


if __name__ == '__main__':
    logging.basicConfig(loglevel=logging.DEBUG)
    main()
