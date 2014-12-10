from __future__ import absolute_import

import logging
import redis


class RedisListHandler(logging.Handler):
    """
    Modified from https://github.com/jedp/python-redis-log

    Publish messages to redis a redis list.

    As a convenience, the classmethod to() can be used as a
    constructor, just as in Andrei Savu's mongodb-log handler.

    If max_messages is set, trim the list to this many items.
    """

    @classmethod
    def to(cls, list_name, max_messages=None, host='localhost', port=6379, level=logging.NOTSET):
        return cls(list_name, max_messages, redis.Redis(host=host, port=port), level=level)

    def __init__(self, list_name, formatter=None, filters=None, max_messages=None,
                 redis_client=None, host='localhost', port=6379,
                 level=logging.NOTSET):
        """
        Create a new logger for the given key and redis_client.
        """
        logging.Handler.__init__(self, level)
        self.list_name = list_name
        self.redis_client = redis_client if redis_client else redis.Redis(host=host, port=port)
        self.max_messages = max_messages
        if formatter:
            self.formatter = formatter
        if filters:
            self.filters = filters

    def emit(self, record):
        """
        Publish record to redis logging list
        """
        try:
            if self.max_messages:
                p = self.redis_client.pipeline()
                p.rpush(self.list_name, self.format(record))
                p.ltrim(self.list_name, -self.max_messages, -1)
                p.execute()
            else:
                self.redis_client.rpush(self.list_name, self.format(record))
        except redis.RedisError:
            pass
