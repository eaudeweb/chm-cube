import sys
import logging
import simplejson as json


def main():
    from to_cube import CubeUploader
    cube = CubeUploader("http://localhost:1080/1.0/event/put")

    bytes_offset = 0

    for line in sys.stdin:
        event = json.loads(line)
        if 'logtail_end' in event:
            get_more = event['logtail_has_more']
            bytes_offset = event['logtail_bytes_offset']
            break

        event['data']['logtail'] = True
        cube.add(event)

    else:
        raise ValueError("json stream ended unexpectedly")

    cube.flush()

    print json.dumps({
        'bytes_offset': bytes_offset,
        'get_more': get_more,
    })


if __name__ == '__main__':
    logging.basicConfig(loglevel=logging.DEBUG)
    main()
