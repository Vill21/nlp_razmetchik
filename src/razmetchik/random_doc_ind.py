import couchdb
import random
import redis
random.seed(13)


if __name__ == '__main__':
    couch = couchdb.Server(f'http://admin:210921@127.0.0.1:5984/')
    db = couch['anek_dataset']

    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    ind = random.randint(0, 121658)
    r.set('id:foo', ind)

    print(db[f'anek_{ind}'])
    print(r.get('id:foo'))
    print(type(r.lrange('id:0', 0, -1)))
