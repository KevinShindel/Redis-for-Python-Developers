import datetime
import random
from redis.client import Redis

from redisolar.dao.base import RateLimiterDaoBase
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.dao.redis.key_schema import KeySchema
from redisolar.dao.base import RateLimitExceededException


class SlidingWindowRateLimiter(RateLimiterDaoBase, RedisDaoBase):
    """A sliding-window rate-limiter."""
    def __init__(self,
                 window_size_ms: float,
                 max_hits: int,
                 redis_client: Redis,
                 key_schema: KeySchema = None,
                 **kwargs):
        self.window_size_ms = window_size_ms
        self.max_hits = max_hits
        self.ip = kwargs.get('ip', None) or self._set_ip()
        super().__init__(redis_client, key_schema, **kwargs)

    def _get_key(self, name, window_size_ms, max_hits):
        return self.key_schema.sliding_window_rate_limiter_key(name, window_size_ms, max_hits)

    def _get_ip_key(self, timestamp):
        return '%f:%s' % (timestamp, self.ip)

    @staticmethod
    def _set_ip():
        ip = '%d.%d.%d.%d' % (random.randint(0, 128), random.randint(0, 128), random.randint(0, 128), random.randint(0, 128))
        return ip

    def hit(self, name: str):
        """Record a hit using the rate-limiter."""
        now = datetime.datetime.now()
        max_timestamp = (now - datetime.timedelta(milliseconds=self.window_size_ms)).timestamp() * 1000
        key = self._get_key(name, self.window_size_ms, self.max_hits)

        pipeline = self.redis.pipeline(transaction=False)

        pipeline.zadd(key, mapping={self._get_ip_key(now.timestamp()): now.timestamp()})
        pipeline.zremrangebyscore(key, min=0, max=max_timestamp)
        pipeline.zcard(key)

        *_, max_hits = pipeline.execute()

        if max_hits >= self.max_hits:
            raise RateLimitExceededException
