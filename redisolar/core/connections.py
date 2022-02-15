import os

import redis
from redistimeseries.client import Client

USERNAME = os.environ.get('REDISOLAR_REDIS_USERNAME', None)
PASSWORD = os.environ.get('REDISOLAR_REDIS_PASSWORD', None)
HOST = os.environ.get('REDISOLAR_REDIS_HOST', '127.0.0.1')
PORT = os.environ.get('REDISOLAR_REDIS_PORT', 6379)


def get_redis_connection(hostname=HOST, port=PORT, username=USERNAME, password=PASSWORD):
    client_kwargs = {
        "host": hostname,
        "port": port,
        "decode_responses": True,
        "max_connections": 100
    }
    if password:
        client_kwargs["password"] = password
    if username:
        client_kwargs["username"] = username

    return redis.Redis(**client_kwargs)


def get_redis_timeseries_connection(hostname, port, username=USERNAME, password=PASSWORD):
    client_kwargs = {
        "host": hostname,
        "port": port,
        "decode_responses": True
    }
    if password:
        client_kwargs["password"] = password
    if username:
        client_kwargs["username"] = username

    return Client(**client_kwargs)
