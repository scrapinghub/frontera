# -*- coding: utf-8 -*-
from collections import Iterable
from datetime import datetime
from frontera.utils.url import parse_domain_from_url_fast
from frontera import DistributedBackend
from frontera.core.components import Metadata, Queue, States
from frontera.core.models import Request
from frontera.contrib.backends.partitioners import Crc32NamePartitioner
from frontera.utils.misc import get_crc32
from frontera.contrib.backends.remote.codecs.msgpack import Decoder, Encoder
import logging
from msgpack import packb, unpackb
from redis import ConnectionPool, StrictRedis
from time import time

FIELD_CRAWL_AT = b'crawl_at'
FIELD_CREATED_AT = b'created_at'
FIELD_DEPTH = b'depth'
FIELD_DOMAIN = b'domain'
FIELD_DOMAIN_FINGERPRINT = b'domain_fingerprint'
FIELD_ERROR = b'error'
FIELD_FINGERPRINT = b'fingerprint'
FIELD_NAME = b'name'
FIELD_SCORE = b'score'
FIELD_STATE = b'state'
FIELD_STATUS_CODE = b'status_code'
FIELD_URL = b'url'

logging.getLogger('boto3.resources.action').setLevel(logging.WARNING)


class RedisQueue(Queue):
    MAX_SCORE = 1.0
    MIN_SCORE = 0.0
    SCORE_STEP = 0.01

    def __init__(self, pool, partitions, delete_all_keys=False):
        self._pool = pool
        self._partitions = [i for i in range(0, partitions)]
        self._partitioner = Crc32NamePartitioner(self._partitions)
        self._logger = logging.getLogger("redis.queue")

        if delete_all_keys:
            connection = StrictRedis(connection_pool=self._pool)
            connection.flushdb()

        class DumbResponse:
            pass

        self._decoder = Decoder(Request, DumbResponse)
        self._encoder = Encoder(Request)

    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        """
        Tries to get new batch from priority queue. It makes self.GET_RETRIES tries and stops, trying to fit all
        parameters. Every new iteration evaluates a deeper batch. After batch is requested it is removed from the queue.
        :param max_n_requests: maximum number of requests
        :param partition_id: partition id to get batch from
        :return: list of :class:`Request <frontera.core.models.Request>` objects.
        """
        min_requests = kwargs.pop('min_requests')
        max_requests_per_host = kwargs.pop('max_requests_per_host')
        assert (max_n_requests >= min_requests)
        connection = StrictRedis(connection_pool=self._pool)
        queue = {}
        count = 0
        now_ts = int(time())
        max_host_items = 0
        to_remove = []
        for data in connection.zrevrange(partition_id, start=0, end=max_n_requests):
            item = unpackb(data, use_list=False)
            timestamp, fprint, host_crc32, _, score = item
            if timestamp > now_ts:
                continue
            if host_crc32 not in queue:
                queue[host_crc32] = []
            if max_requests_per_host is not None and len(queue[host_crc32]) > max_requests_per_host:
                continue
            queue[host_crc32].append(item)
            if len(queue[host_crc32]) > max_host_items:
                max_host_items = len(queue[host_crc32])
            count += 1
            to_remove.append(data)

            if count >= max_n_requests:
                break

        self._logger.debug("Finished: hosts {}, requests {}".format(len(queue.keys()), count))

        results = []
        for i in range(max_host_items):
            for host_crc32, items in queue.items():
                if len(items) <= i:
                    continue
                item = items[i]
                (_, _, _, encoded, score) = item
                to_remove.append(packb(item))
                request = self._decoder.decode_request(encoded)
                request.meta[FIELD_SCORE] = score
                results.append(request)
        if len(to_remove) > 0:
            connection.zrem(partition_id, *to_remove)
        return results

    def schedule(self, batch):
        to_schedule = dict()
        now = int(time())
        for fprint, score, request, schedule in batch:
            if schedule:
                # TODO: This is done by DomainMiddleware - RedisBackend should require DomainMiddleware
                if FIELD_DOMAIN not in request.meta:
                    _, hostname, _, _, _, _ = parse_domain_from_url_fast(request.url)
                    if not hostname:
                        self._logger.error("Can't get hostname for URL %s, fingerprint %s", request.url, fprint)
                    request.meta[FIELD_DOMAIN] = {'name': hostname}
                timestamp = request.meta[FIELD_CRAWL_AT] if FIELD_CRAWL_AT in request.meta else now
                to_schedule.setdefault(timestamp, []).append((request, score))
        for timestamp, batch in to_schedule.items():
            self._schedule(batch, timestamp)

    @classmethod
    def get_interval_start(cls, score):
        if score < cls.MIN_SCORE or score > cls.MAX_SCORE:
            raise OverflowError
        i = int(score / cls.SCORE_STEP)
        if i % 10 == 0 and i > 0:
            i -= 1  # last interval is inclusive from right
        return i * cls.SCORE_STEP

    def _schedule(self, batch, timestamp):
        data = dict()
        for request, score in batch:
            domain = request.meta[FIELD_DOMAIN]
            fingerprint = request.meta[FIELD_FINGERPRINT]
            if type(domain) == dict:
                partition_id = self._partitioner.partition(domain[FIELD_NAME], self._partitions)
                host_crc32 = get_crc32(domain[FIELD_NAME])
            elif type(domain) == int:
                partition_id = self._partitioner.partition_by_hash(domain, self._partitions)
                host_crc32 = domain
            else:
                raise TypeError("domain of unknown type.")
            item = (timestamp, fingerprint, host_crc32, self._encoder.encode_request(request), score)
            interval_start = self.get_interval_start(score)
            data.setdefault(partition_id, []).extend([int(interval_start * 100), packb(item)])
        connection = StrictRedis(connection_pool=self._pool)
        pipe = connection.pipeline()
        for key, items in data.items():
            connection.zadd(key, *items)
        pipe.execute()

    def count(self):
        connection = StrictRedis(connection_pool=self._pool)
        count = 0
        for partition_id in self._partitions:
            count += connection.zcard(partition_id)
        return count

    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass


class RedisState(States):
    def __init__(self, pool, cache_size_limit):
        self._pool = pool
        self._cache = {}
        self._cache_size_limit = cache_size_limit
        self._logger = logging.getLogger("redis.states")

    def update_cache(self, objs):
        objs = objs if isinstance(objs, Iterable) else [objs]

        def put(obj):
            self._cache[obj.meta[FIELD_FINGERPRINT]] = obj.meta[FIELD_STATE]

        [put(obj) for obj in objs]

    def set_states(self, objs):
        objs = objs if isinstance(objs, Iterable) else [objs]

        def get(obj):
            fprint = obj.meta[FIELD_FINGERPRINT]
            obj.meta[FIELD_STATE] = self._cache[fprint] if fprint in self._cache else States.DEFAULT

        [get(obj) for obj in objs]

    def flush(self, force_clear):
        if len(self._cache) > self._cache_size_limit:
            force_clear = True
        connection = StrictRedis(connection_pool=self._pool)
        pipe = connection.pipeline()
        for fprint, state in self._cache.items():
            pipe.hmset(fprint, {FIELD_STATE: state})
        pipe.execute()
        if force_clear:
            self._logger.debug("Cache has %d requests, clearing" % len(self._cache))
            self._cache.clear()

    def fetch(self, fingerprints):
        to_fetch = [f for f in fingerprints if f not in self._cache]
        self._logger.debug("cache size %s" % len(self._cache))
        self._logger.debug("to fetch %d from %d" % (len(to_fetch), len(fingerprints)))
        connection = StrictRedis(connection_pool=self._pool)
        pipe = connection.pipeline()
        for key in to_fetch:
            pipe.hgetall(key)
        responses = pipe.execute()
        for index, key in enumerate(to_fetch):
            response = responses[index]
            if len(response) > 0 and FIELD_STATE in response:
                self._cache[key] = response[FIELD_STATE]
            else:
                self._cache[key] = self.NOT_CRAWLED

    def frontier_start(self):
        pass

    def frontier_stop(self):
        self.flush(False)


class RedisMetadata(Metadata):
    def __init__(self, pool, delete_all_keys):
        self._pool = pool
        self._logger = logging.getLogger("redis.metadata")
        if delete_all_keys:
            connection = StrictRedis(connection_pool=self._pool)
            connection.flushdb()

    @classmethod
    def timestamp(cls):
        return str(datetime.utcnow().replace(microsecond=0))

    def add_seeds(self, seeds):
        connection = StrictRedis(connection_pool=self._pool)
        pipe = connection.pipeline()
        for seed in seeds:
            pipe.hmset(
                seed.meta[FIELD_FINGERPRINT],
                {
                    FIELD_URL: seed.url,
                    FIELD_DEPTH: 0,
                    FIELD_CREATED_AT: self.timestamp(),
                    FIELD_DOMAIN_FINGERPRINT: seed.meta[FIELD_DOMAIN][FIELD_FINGERPRINT]
                }
            )
        pipe.execute()

    def request_error(self, page, error):
        connection = StrictRedis(connection_pool=self._pool)
        connection.hmset(
            page.meta[FIELD_FINGERPRINT],
            {
                FIELD_URL: page.url,
                FIELD_CREATED_AT: self.timestamp(),
                FIELD_ERROR: error,
                FIELD_DOMAIN_FINGERPRINT: page.meta[FIELD_DOMAIN][FIELD_FINGERPRINT]
            }
        )

    def page_crawled(self, response):
        connection = StrictRedis(connection_pool=self._pool)
        connection.hmset(
            response.meta[FIELD_FINGERPRINT],
            {
                FIELD_STATUS_CODE: response.status_code
            }
        )

    def links_extracted(self, _, links):
        links_processed = set()
        connection = StrictRedis(connection_pool=self._pool)
        for link in links:
            link_fingerprint = link.meta[FIELD_FINGERPRINT]
            if link_fingerprint in links_processed:
                continue
            connection.hmset(
                link_fingerprint,
                {
                    FIELD_URL: link.url,
                    FIELD_CREATED_AT: self.timestamp(),
                    FIELD_DOMAIN_FINGERPRINT: link.meta[FIELD_DOMAIN][FIELD_FINGERPRINT]
                }
            )
            links_processed.add(link_fingerprint)

    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass


class RedisBackend(DistributedBackend):
    component_name = 'Redis Backend'

    def __init__(self, manager):
        self.manager = manager
        self._logger = logging.getLogger("redis.backend")
        settings = manager.settings
        port = settings.get('REDIS_PORT')
        host = settings.get('REDIS_HOST')
        self._min_requests = settings.get('BC_MIN_REQUESTS')
        self._min_hosts = settings.get('BC_MIN_HOSTS')
        self._max_requests_per_host = settings.get('BC_MAX_REQUESTS_PER_HOST')

        self.queue_partitions = settings.get('SPIDER_FEED_PARTITIONS')
        self._logger.info("RedisBackend started with {} partitions".format(self.queue_partitions))
        self.pool = ConnectionPool(host=host, port=port, db=0)
        self._metadata = None
        self._queue = None
        self._states = None

    @classmethod
    def strategy_worker(cls, manager):
        o = cls(manager)
        settings = manager.settings
        o._states = RedisState(o.pool, settings.get('REDIS_STATE_CACHE_SIZE_LIMIT'))
        return o

    @classmethod
    def db_worker(cls, manager):
        o = cls(manager)
        settings = manager.settings
        clear = settings.get('REDIS_DROP_ALL_TABLES')
        o._queue = RedisQueue(o.pool, o.queue_partitions, delete_all_keys=clear)
        o._metadata = RedisMetadata(
            o.pool,
            clear
        )
        return o

    @property
    def metadata(self):
        return self._metadata

    @property
    def queue(self):
        return self._queue

    @property
    def states(self):
        return self._states

    def frontier_start(self):
        for component in [self.metadata, self.queue, self.states]:
            if component:
                component.frontier_start()

    def frontier_stop(self):
        for component in [self.metadata, self.queue, self.states]:
            if component:
                component.frontier_stop()
        self.pool.disconnect()

    def add_seeds(self, seeds):
        self.metadata.add_seeds(seeds)

    def page_crawled(self, response):
        self.metadata.page_crawled(response)

    def links_extracted(self, request, links):
        self.metadata.links_extracted(request, links)

    def request_error(self, page, error):
        self.metadata.request_error(page, error)

    def finished(self):
        raise NotImplementedError

    def get_next_requests(self, max_next_requests, **kwargs):
        next_pages = []
        self._logger.debug("Querying queue table.")
        partitions = set(kwargs.pop('partitions', []))
        for partition_id in range(0, self.queue_partitions):
            if partition_id not in partitions:
                continue
            results = self.queue.get_next_requests(max_next_requests, partition_id,
                                                   min_requests=self._min_requests,
                                                   min_hosts=self._min_hosts,
                                                   max_requests_per_host=self._max_requests_per_host)
            next_pages.extend(results)
            self._logger.debug("Got %d requests for partition id %d", len(results), partition_id)
        return next_pages
