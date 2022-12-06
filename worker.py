import os

import redis
from rq import Worker, Queue, Connection

listen = ['high', 'default', 'low']

redis_url = os.getenv('REDIS_URL', 'redis://:pd31c532a190214607209de10ed11d7df9c3814c3809bf49b04df3e32ddbf8781@ec2-52-86-226-56.compute-1.amazonaws.com:30769')

conn = redis.from_url(redis_url)

if __name__ == '__main__':
    with Connection(conn):
        worker = Worker(map(Queue, listen))
        worker.work()
