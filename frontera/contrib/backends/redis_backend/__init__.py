# -*- coding: utf-8 -*-
from collections import Iterable
from datetime import datetime
from frontera.utils.url import parse_domain_from_url_fast
from frontera import DistributedBackend
from frontera.core.components import Metadata, Queue, States
from frontera.contrib.backends.partitioners import Crc32NamePartitioner
from frontera.utils.misc import get_crc32, load_object
import logging
from msgpack import packb, unpackb
from redis import ConnectionPool, StrictRedis
from redis.exceptions import ConnectionError, ResponseError
from time import sleep, time

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

"""
Error handling:
* On Connection error:
** Retry three times with increasing timout. 
** Skip the operation if the third retry fails.
* On Response error:
** Report and continue.
** Reponse error is usually caused by Redis using all available memory. Ideally, Redis should have enough memory
 for this not to happen. Still, if Redis is full, the rest of the crawler may continue and free up some space in 
 Redis after a while.
"""


class RedisOperations:
    @classmethod
    def _get_retry_timeouts(cls):
        # Timeout generator with backoff until 60 seconds
        for timeout in [0, 10, 30]: yield timeout
        yield None

    def _redis_operation(self, message, operation, *args):
        timeout = self._get_retry_timeouts()
        while True:
            try:
                connection = StrictRedis(connection_pool=self._pool)
                return operation(connection, *args)
            except ConnectionError as e:
                self._logger.exception("Connection to Redis failed when attempting to {0}".format(message))
                pause = timeout.next()
                if pause == None:
                    break
                sleep(pause)
            except ResponseError as e:
                self._logger.exception("Writing to Redis failed when attempting to {0}".format(message))
                break

    def _redis_pipeline(self, message, operation, *args):
        timeout = self._get_retry_timeouts()
        while True:
            try:
                connection = StrictRedis(connection_pool=self._pool)
                pipe = connection.pipeline()
                operation(pipe, *args)
                return pipe.execute()
            except ConnectionError as e:
                self._exception("Connection to Redis failed when attempting to {0}".format(message))
                pause = timeout.next()
                if pause == None:
                    break
                sleep(pause)
            except ResponseError as e:
                self._logger.exception("Writing to Redis failed when attempting to {0}".format(message))
                break


class RedisQueue(Queue, RedisOperations):
    MAX_SCORE = 1.0
    MIN_SCORE = 0.0
    SCORE_STEP = 0.01

    def __init__(self, manager, pool, partitions, delete_all_keys=False):
        settings = manager.settings
        codec_path = settings.get('REDIS_BACKEND_CODEC')
        encoder_cls = load_object(codec_path + ".Encoder")
        decoder_cls = load_object(codec_path + ".Decoder")
        self._encoder = encoder_cls(manager.request_model)
        self._decoder = decoder_cls(manager.request_model, manager.response_model)
        self._pool = pool
        self._partitions = [i for i in range(0, partitions)]
        self._partitioner = Crc32NamePartitioner(self._partitions)
        self._logger = logging.getLogger("redis_backend.queue")

        if delete_all_keys:
            self._redis_operation( "flushing db", lambda connection: connection.flushdb())

    def _get_items(self, connection, partition_id, start, now_ts, queue, max_requests_per_host, max_host_items, count, max_n_requests, to_remove):
        for data in connection.zrevrange(partition_id, start=start, end=max_n_requests + start):
            start += 1
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
        return start, count, max_host_items

    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        """
        Fetch new batch from priority queue.
        :param max_n_requests: maximum number of requests
        :param partition_id: partition id to get batch from
        :return: list of :class:`Request <frontera.core.models.Request>` objects.
        """
        max_requests_per_host = kwargs.pop('max_requests_per_host')
        queue = {}
        count = 0
        now_ts = int(time())
        max_host_items = 0
        to_remove = []
        start = 0
        last_start = -1
        while count < max_n_requests and last_start < start:
            last_start = start
            start, count, max_host_items = self._redis_operation(
                "get more requests",
                lambda connection: self._get_items(
                    connection, partition_id, start, now_ts, queue, max_requests_per_host, max_host_items, count,
                    max_n_requests, to_remove)
            )
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
            self._redis_operation(
                "remove scheduled items", lambda connection: connection.zrem(partition_id, *to_remove)
            )
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
            self._redis_pipeline(
                "schedule items",
                lambda pipe, data: map(lambda (key, items): pipe.zadd(key, *items), data.items()), data
            )

    def count(self):
        return self._redis_operation(
            "count items",
            lambda connection, partitions: sum(map(lambda partition_id: connection.zcard(partition_id), partitions)),
            self._partitions
        )

    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass


class RedisState(States, RedisOperations):
    def __init__(self, pool, cache_size_limit):
        self._pool = pool
        self._cache = {}
        self._cache_size_limit = cache_size_limit
        self._logger = logging.getLogger("redis_backend.states")

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
        self._redis_pipeline(
            "flush cache",
            lambda pipe, cache: map(lambda (fprint, state): pipe.hmset(fprint, {FIELD_STATE: state}), cache.items()),
            self._cache
        )
        if force_clear:
            self._logger.debug("Cache has %d requests, clearing" % len(self._cache))
            self._cache.clear()

    def fetch(self, fingerprints):
        to_fetch = [f for f in fingerprints if f not in self._cache]
        self._logger.debug("cache size %s" % len(self._cache))
        self._logger.debug("to fetch %d from %d" % (len(to_fetch), len(fingerprints)))
        responses = self._redis_pipeline(
            "fetch fingerprints",
            lambda pipe, to_fetch: map(lambda key: pipe.hgetall(key), to_fetch), to_fetch
        )
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


class RedisMetadata(Metadata, RedisOperations):
    def __init__(self, pool, delete_all_keys):
        self._pool = pool
        self._logger = logging.getLogger("redis_backend.metadata")
        if delete_all_keys:
            self._redis_operation("flush database", lambda connection: connection.flushdb())

    @classmethod
    def timestamp(cls):
        return str(datetime.utcnow().replace(microsecond=0))

    def _create_seed(self, seed):
        return {
            FIELD_URL: seed.url,
            FIELD_DEPTH: 0,
            FIELD_CREATED_AT: self.timestamp(),
            FIELD_DOMAIN_FINGERPRINT: seed.meta[FIELD_DOMAIN][FIELD_FINGERPRINT]
        }

    def add_seeds(self, seeds):
        self._redis_pipeline(
            "add seeds",
            lambda pipe, seeds: map(
                lambda seed: pipe.hmset(seed.meta[FIELD_FINGERPRINT], self._create_seed(seed)), seeds), seeds
        )

    def _create_request_error(self, page, error):
        return {
            FIELD_URL: page.url,
            FIELD_CREATED_AT: self.timestamp(),
            FIELD_ERROR: error,
            FIELD_DOMAIN_FINGERPRINT: page.meta[FIELD_DOMAIN][FIELD_FINGERPRINT]
        }

    def request_error(self, page, error):
        self._redis_operation(
            "write requests error",
            lambda connection, page, error: connection.hmset(page.meta[FIELD_FINGERPRINT], self._create_request_error(page, error)), page, error
        )

    def _create_crawl_info(self, response):
        return {
            FIELD_STATUS_CODE: response.status_code
        }

    def page_crawled(self, response):
        self._redis_operation(
            "write page crawled status",
            lambda connection, response: connection.hmset(response.meta[FIELD_FINGERPRINT], self._create_crawl_info(response)), response
        )

    def _create_link_extracted(self, link):
        return {
            FIELD_URL: link.url,
            FIELD_CREATED_AT: self.timestamp(),
            FIELD_DOMAIN_FINGERPRINT: link.meta[FIELD_DOMAIN][FIELD_FINGERPRINT]
        }

    def links_extracted(self, _, links):
        links_deduped = {}
        for link in links:
            link_fingerprint = link.meta[FIELD_FINGERPRINT]
            if link_fingerprint in links_deduped:
                continue
            links_deduped[link_fingerprint] = link
        self._redis_pipeline(
            "write links extracted",
            lambda pipe, links: map(
                lambda (fingerprint, link): pipe.hmset(fingerprint, self._create_link_extracted(link)), links.items()),
            links_deduped
        )

    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass


class RedisBackend(DistributedBackend):
    component_name = 'Redis Backend'

    def __init__(self, manager):
        self.manager = manager
        self._logger = logging.getLogger("redis_backend.backend")
        settings = manager.settings
        port = settings.get('REDIS_PORT')
        host = settings.get('REDIS_HOST')
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
        o._queue = RedisQueue(manager, o.pool, o.queue_partitions, delete_all_keys=clear)
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
        for partition_id in partitions:
            results = self.queue.get_next_requests(max_next_requests, partition_id,
                                                   min_hosts=self._min_hosts,
                                                   max_requests_per_host=self._max_requests_per_host)
            next_pages.extend(results)
            self._logger.debug("Got %d requests for partition id %d", len(results), partition_id)
        return next_pages
