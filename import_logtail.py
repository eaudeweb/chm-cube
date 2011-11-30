import sys
import logging
import threading
import time
import simplejson as json


FLUSH_INTERVAL = 10 # seconds


class FlushThread(threading.Thread):
    def __init__(self, manual_flush):
        super(FlushThread, self).__init__()
        self.manual_flush = manual_flush
        self.stop = False

    def run(self):
        while not self.stop:
            self.manual_flush()
            time.sleep(FLUSH_INTERVAL)


def main():
    from to_cube import CubeUploader
    cube = CubeUploader("http://localhost:1080/1.0/event/put")
    flush_lock = threading.Lock()

    state = {'skip': None, 'flushed_skip': None}

    def manual_flush():
        with flush_lock:
            cube.flush()
            state['flushed_skip'] = state['skip']

    flush_thread = FlushThread(manual_flush)
    flush_thread.start()

    try:
        while True:
            line = sys.stdin.readline()
            if not line:
                break
            event = json.loads(line)
            event['data']['logtail'] = True
            state['skip'] = event.pop('_skip')
            with flush_lock:
                cube.add(event)

    except KeyboardInterrupt:
        pass

    finally:
        flush_thread.stop = True
        manual_flush()
        if state['flushed_skip'] is not None:
            print state['flushed_skip']
        flush_thread.join()


if __name__ == '__main__':
    logging.basicConfig(loglevel=logging.DEBUG)
    main()
