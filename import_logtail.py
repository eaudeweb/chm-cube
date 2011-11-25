import sys
import logging
import simplejson as json


def main():
    from to_cube import CubeUploader
    cube = CubeUploader("http://localhost:1080/1.0/event/put")

    last_id = None
    for line in sys.stdin:
        event = json.loads(line)
        if event is None:
            get_more = False
            break

        cube.add(event)
        last_id = event['id']

    else:
        get_more = True

    cube.flush()

    print json.dumps({
        'last_id': last_id,
        'get_more': get_more,
    })


if __name__ == '__main__':
    logging.basicConfig(loglevel=logging.DEBUG)
    main()
