from datetime import time, datetime, timedelta
import pymongo


def main():
    mongo = pymongo.Connection()
    db = mongo['cube_development']
    events = db['chm_eu_events']

    now = datetime.utcnow()
    #t_start = now - timedelta(hours=1)
    t_start = datetime.combine(now.date(), time())

    query = {
        't': {'$gt': t_start},
        'd.status': {'$gte': 500},
    }
    for event in db.chm_eu_events.find(query):
        print '%(status)d http://%(vhost)s%(path)s' % event['d']


if __name__ == '__main__':
    main()
