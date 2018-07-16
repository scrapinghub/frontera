# -*- coding: utf-8 -*-
from __future__ import absolute_import, division
from frontera import DistributedBackend
from frontera.core.components import Metadata, Queue, States
from frontera.core.models import Request
from frontera.contrib.backends.partitioners import Crc32NamePartitioner
from frontera.utils.misc import chunks, get_crc32, time_elapsed
from frontera.contrib.backends.remote.codecs.msgpack import Decoder, Encoder
from frontera.contrib.backends.hbase.domaincache import DomainCache

from happybase import Connection
from msgpack import Unpacker, Packer, packb
import six
from six.moves import range
from w3lib.util import to_bytes
from cachetools import LRUCache

from struct import pack, unpack
from datetime import datetime
from calendar import timegm
from time import time
from binascii import hexlify, unhexlify
from io import BytesIO
from random import choice
from collections import defaultdict, Iterable
import logging

_pack_functions = {
    'url': to_bytes,
    'depth': lambda x: pack('>I', 0),
    'created_at': lambda x: pack('>Q', x),
    'status_code': lambda x: pack('>H', x),
    'state': lambda x: pack('>B', x),
    'error': to_bytes,
    'domain_fprint': to_bytes,
    'score': lambda x: pack('>f', x),
    'content': to_bytes,
    'headers': packb,
    'dest_fprint': to_bytes
}


def unpack_score(blob):
    return unpack(">d", blob)[0]


def prepare_hbase_object(obj=None, **kwargs):
    if not obj:
        obj = dict()
    for k, v in six.iteritems(kwargs):
        if k in ['score', 'state']:
            cf = 's'
        elif k == 'content':
            cf = 'c'
        else:
            cf = 'm'
        func = _pack_functions[k]
        obj[cf + ':' + k] = func(v)
    return obj


def utcnow_timestamp():
    d = datetime.utcnow()
    return timegm(d.timetuple())


class LRUCacheWithStats(LRUCache):
    """Extended version of standard LRUCache with counting stats."""

    EVICTED_STATNAME = 'states.cache.evicted'

    def __init__(self, stats=None, *args, **kwargs):
        super(LRUCacheWithStats, self).__init__(*args, **kwargs)
        self._stats = stats
        if self._stats is not None:
            self._stats.setdefault(self.EVICTED_STATNAME, 0)

    def popitem(self):
        key, val = super(LRUCacheWithStats, self).popitem()
        if self._stats:
            self._stats[self.EVICTED_STATNAME] += 1
        return key, val


class HBaseQueue(Queue):
    GET_RETRIES = 3

    def __init__(self, connection, partitions, table_name, drop=False, use_snappy=False):
        self.connection = connection
        self.partitions = [i for i in range(0, partitions)]
        self.partitioner = Crc32NamePartitioner(self.partitions)
        self.logger = logging.getLogger("hbase.queue")
        self.table_name = to_bytes(table_name)

        tables = set(self.connection.tables())
        if drop and self.table_name in tables:
            self.connection.delete_table(self.table_name, disable=True)
            tables.remove(self.table_name)

        if self.table_name not in tables:
            schema = {'f': {'max_versions': 1}}
            if use_snappy:
                schema['f']['compression'] = 'SNAPPY'
            self.connection.create_table(self.table_name, schema)

        class DumbResponse:
            pass

        self.decoder = Decoder(Request, DumbResponse)
        self.encoder = Encoder(Request)

    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass

    def schedule(self, batch):
        to_schedule = dict()
        now = int(time())
        for fprint, score, request, schedule in batch:
            if schedule:
                assert b'domain' in request.meta
                timestamp = request.meta[b'crawl_at'] if b'crawl_at' in request.meta else now
                to_schedule.setdefault(timestamp, []).append((request, score))
        for timestamp, batch in six.iteritems(to_schedule):
            self._schedule(batch, timestamp)

    def _schedule(self, batch, timestamp):
        """
        Row - portion of the queue for each partition id created at some point in time
        Row Key - partition id + score interval + random_str
        Column Qualifier - discrete score (first three digits after dot, e.g. 0.001_0.002, 0.002_0.003, ...)
        Value - QueueCell msgpack blob

        Where score is mapped from 0.0 to 1.0
        score intervals are
          [0.01-0.02)
          [0.02-0.03)
          [0.03-0.04)
         ...
          [0.99-1.00]
        random_str - the time when links was scheduled for retrieval, microsecs

        :param batch: iterable of Request objects
        :return:
        """

        def get_interval(score, resolution):
            if score < 0.0 or score > 1.0:
                raise OverflowError

            i = int(score / resolution)
            if i % 10 == 0 and i > 0:
                i = i - 1  # last interval is inclusive from right
            return (i * resolution, (i + 1) * resolution)

        random_str = int(time() * 1E+6)
        data = dict()
        for request, score in batch:
            domain = request.meta[b'domain']
            fingerprint = request.meta[b'fingerprint']
            slot = request.meta.get(b'slot')
            if slot is not None:
                partition_id = self.partitioner.partition(slot, self.partitions)
                key_crc32 = get_crc32(slot)
            elif type(domain) == dict:
                partition_id = self.partitioner.partition(domain[b'name'], self.partitions)
                key_crc32 = get_crc32(domain[b'name'])
            elif type(domain) == int:
                partition_id = self.partitioner.partition_by_hash(domain, self.partitions)
                key_crc32 = domain
            else:
                raise TypeError("partitioning key and info isn't provided")
            item = (unhexlify(fingerprint), key_crc32, self.encoder.encode_request(request), score)
            score = 1 - score  # because of lexicographical sort in HBase
            rk = "%d_%s_%d" % (partition_id, "%0.2f_%0.2f" % get_interval(score, 0.01), random_str)
            data.setdefault(rk, []).append((score, item))

        table = self.connection.table(self.table_name)
        with table.batch(transaction=True) as b:
            for rk, tuples in six.iteritems(data):
                obj = dict()
                for score, item in tuples:
                    column = 'f:%0.3f_%0.3f' % get_interval(score, 0.001)
                    obj.setdefault(column, []).append(item)

                final = dict()
                packer = Packer()
                for column, items in six.iteritems(obj):
                    stream = BytesIO()
                    for item in items:
                        stream.write(packer.pack(item))
                    final[column] = stream.getvalue()
                final[b'f:t'] = str(timestamp)
                b.put(rk, final)

    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        """
        Tries to get new batch from priority queue. It makes self.GET_RETRIES tries and stops, trying to fit all
        parameters. Every new iteration evaluates a deeper batch. After batch is requested it is removed from the queue.

        :param max_n_requests: maximum number of requests
        :param partition_id: partition id to get batch from
        :param min_requests: minimum number of requests
        :param min_hosts: minimum number of hosts
        :param max_requests_per_host: maximum number of requests per host
        :return: list of :class:`Request <frontera.core.models.Request>` objects.
        """
        min_requests = kwargs.pop('min_requests')
        min_hosts = kwargs.pop('min_hosts', None)
        max_requests_per_host = kwargs.pop('max_requests_per_host', None)
        assert (max_n_requests > min_requests)
        table = self.connection.table(self.table_name)

        meta_map = {}
        queue = {}
        limit = min_requests
        tries = 0
        count = 0
        prefix = to_bytes('%d_' % partition_id)
        # now_ts = int(time())
        # TODO: figure out how to use filter here, Thrift filter above causes full scan
        # filter = "PrefixFilter ('%s') AND SingleColumnValueFilter ('f', 't', <=, 'binary:%d')" % (prefix, now_ts)
        while tries < self.GET_RETRIES:
            tries += 1
            limit *= 5.5 if tries > 1 else 1.0
            self.logger.debug("Try %d, limit %d, last attempt: requests %d, hosts %d",
                              tries, limit, count, len(queue.keys()))
            meta_map.clear()
            queue.clear()
            count = 0
            # XXX pypy hot-fix: non-exhausted generator must be closed manually
            # otherwise "finally" piece in table.scan() method won't be executed
            # immediately to properly close scanner (http://pypy.org/compat.html)
            scan_gen = table.scan(limit=int(limit), batch_size=256, row_prefix=prefix, sorted_columns=True)
            try:
                for rk, data in scan_gen:
                    for cq, buf in six.iteritems(data):
                        if cq == b'f:t':
                            continue
                        stream = BytesIO(buf)
                        unpacker = Unpacker(stream)
                        for item in unpacker:
                            fprint, key_crc32, _, _ = item
                            if key_crc32 not in queue:
                                queue[key_crc32] = []
                            if max_requests_per_host is not None and len(queue[key_crc32]) > max_requests_per_host:
                                continue
                            queue[key_crc32].append(fprint)
                            count += 1

                            if fprint not in meta_map:
                                meta_map[fprint] = []
                            meta_map[fprint].append((rk, item))
                    if count > max_n_requests:
                        break
            finally:
                scan_gen.close()

            if min_hosts is not None and len(queue.keys()) < min_hosts:
                continue

            if count < min_requests:
                continue
            break

        self.logger.debug("Finished: tries %d, hosts %d, requests %d", tries, len(queue.keys()), count)

        # For every fingerprint collect it's row keys and return all fingerprints from them
        fprint_map = {}
        for fprint, meta_list in six.iteritems(meta_map):
            for rk, _ in meta_list:
                fprint_map.setdefault(rk, []).append(fprint)

        results = []
        trash_can = set()

        for _, fprints in six.iteritems(queue):
            for fprint in fprints:
                for rk, _ in meta_map[fprint]:
                    if rk in trash_can:
                        continue
                    for rk_fprint in fprint_map[rk]:
                        _, item = meta_map[rk_fprint][0]
                        _, _, encoded, score = item
                        request = self.decoder.decode_request(encoded)
                        request.meta[b'score'] = score
                        results.append(request)
                    trash_can.add(rk)

        with table.batch(transaction=True) as b:
            for rk in trash_can:
                b.delete(rk)
        self.logger.debug("%d row keys removed", len(trash_can))
        return results

    def count(self):
        raise NotImplementedError


class HBaseState(States):
    def __init__(self, connection, table_name, cache_size_limit,
                 write_log_size, drop_all_tables):
        self.connection = connection
        self._table_name = to_bytes(table_name)
        self.logger = logging.getLogger("hbase.states")
        self._state_batch = self.connection.table(
            self._table_name).batch(batch_size=write_log_size)
        self._state_stats = defaultdict(int)
        self._state_cache = LRUCacheWithStats(maxsize=cache_size_limit,
                                              stats=self._state_stats)
        self._state_last_updates = 0

        tables = set(connection.tables())
        if drop_all_tables and self._table_name in tables:
            connection.delete_table(self._table_name, disable=True)
            tables.remove(self._table_name)

        if self._table_name not in tables:
            schema = {'s': {'max_versions': 1, 'block_cache_enabled': 1,
                            'bloom_filter_type': 'ROW', 'in_memory': True, }
                      }
            connection.create_table(self._table_name, schema)

    def update_cache(self, objs):
        objs = objs if isinstance(objs, Iterable) else [objs]
        for obj in objs:
            fingerprint, state = obj.meta[b'fingerprint'], obj.meta[b'state']
            # prepare & write state change to happybase batch
            self._state_batch.put(unhexlify(fingerprint), prepare_hbase_object(state=state))
            # update LRU cache with the state update
            self._state_cache[fingerprint] = state
            self._state_last_updates += 1
        self._update_batch_stats()

    def set_states(self, objs):
        objs = objs if isinstance(objs, Iterable) else [objs]
        for obj in objs:
            obj.meta[b'state'] = self._state_cache.get(obj.meta[b'fingerprint'], States.DEFAULT)

    def flush(self):
        self._state_batch.send()

    def fetch(self, fingerprints):
        to_fetch = [f for f in fingerprints if f not in self._state_cache]
        self._update_cache_stats(hits=len(fingerprints) - len(to_fetch),
                                 misses=len(to_fetch))
        if not to_fetch:
            return
        self.logger.debug('Fetching %d/%d elements from HBase (cache size %d)',
                          len(to_fetch), len(fingerprints), len(self._state_cache))
        for chunk in chunks(to_fetch, 65536):
            keys = [unhexlify(fprint) for fprint in chunk]
            table = self.connection.table(self._table_name)
            records = table.rows(keys, columns=[b's:state'])
            for key, cells in records:
                if b's:state' in cells:
                    state = unpack('>B', cells[b's:state'])[0]
                    self._state_cache[hexlify(key)] = state

    def _update_batch_stats(self):
        new_batches_count, self._state_last_updates = divmod(
            self._state_last_updates, self._state_batch._batch_size)
        self._state_stats['states.batches.sent'] += new_batches_count

    def _update_cache_stats(self, hits, misses):
        total_hits = self._state_stats['states.cache.hits'] + hits
        total_misses = self._state_stats['states.cache.misses'] + misses
        total = total_hits + total_misses
        self._state_stats['states.cache.hits'] = total_hits
        self._state_stats['states.cache.misses'] = total_misses
        self._state_stats['states.cache.ratio'] = total_hits / total if total else 0

    def get_stats(self):
        stats = self._state_stats.copy()
        self._state_stats.clear()
        return stats


class HBaseMetadata(Metadata):
    def __init__(self, connection, table_name, drop_all_tables, use_snappy, batch_size, store_content):
        self._table_name = to_bytes(table_name)
        tables = set(connection.tables())
        if drop_all_tables and self._table_name in tables:
            connection.delete_table(self._table_name, disable=True)
            tables.remove(self._table_name)

        if self._table_name not in tables:
            schema = {'m': {'max_versions': 1},
                      'c': {'max_versions': 1}
                      }
            if use_snappy:
                schema['m']['compression'] = 'SNAPPY'
                schema['c']['compression'] = 'SNAPPY'
            connection.create_table(self._table_name, schema)
        table = connection.table(self._table_name)
        self.batch = table.batch(batch_size=batch_size)
        self.store_content = store_content

    def frontier_start(self):
        pass

    def frontier_stop(self):
        self.flush()

    def flush(self):
        self.batch.send()

    def add_seeds(self, seeds):
        for seed in seeds:
            obj = prepare_hbase_object(url=seed.url,
                                       depth=0,
                                       created_at=utcnow_timestamp(),
                                       domain_fprint=seed.meta[b'domain'][b'fingerprint'])
            self.batch.put(unhexlify(seed.meta[b'fingerprint']), obj)

    def page_crawled(self, response):
        headers = response.headers
        redirect_urls = response.request.meta.get(b'redirect_urls')
        redirect_fprints = response.request.meta.get(b'redirect_fingerprints')
        if redirect_urls:
            for url, fprint in zip(redirect_urls, redirect_fprints):
                obj = prepare_hbase_object(url=url,
                                           created_at=utcnow_timestamp(),
                                           dest_fprint=redirect_fprints[-1])
                self.batch.put(fprint, obj)
        obj = prepare_hbase_object(status_code=response.status_code, headers=headers,
                                   content=response.body) if self.store_content else \
            prepare_hbase_object(status_code=response.status_code, headers=headers)
        self.batch.put(unhexlify(response.meta[b'fingerprint']), obj)

    def links_extracted(self, request, links):
        links_dict = dict()
        for link in links:
            links_dict[unhexlify(link.meta[b'fingerprint'])] = (link, link.url, link.meta[b'domain'])
        for link_fingerprint, (link, link_url, link_domain) in six.iteritems(links_dict):
            obj = prepare_hbase_object(url=link_url,
                                       created_at=utcnow_timestamp(),
                                       domain_fprint=link_domain[b'fingerprint'])
            self.batch.put(link_fingerprint, obj)

    def request_error(self, request, error):
        obj = prepare_hbase_object(url=request.url,
                                   created_at=utcnow_timestamp(),
                                   error=error,
                                   domain_fprint=request.meta[b'domain'][b'fingerprint'])
        rk = unhexlify(request.meta[b'fingerprint'])
        self.batch.put(rk, obj)
        if b'redirect_urls' in request.meta:
            for url, fprint in zip(request.meta[b'redirect_urls'], request.meta[b'redirect_fingerprints']):
                obj = prepare_hbase_object(url=url,
                                           created_at=utcnow_timestamp(),
                                           dest_fprint=request.meta[b'redirect_fingerprints'][-1])
                self.batch.put(fprint, obj)

    def update_score(self, batch):
        if not isinstance(batch, dict):
            raise TypeError('batch should be dict with fingerprint as key, and float score as value')
        for fprint, (score, url, schedule) in six.iteritems(batch):
            obj = prepare_hbase_object(score=score)
            rk = unhexlify(fprint)
            self.batch.put(rk, obj)


class HBaseBackend(DistributedBackend):
    component_name = 'HBase Backend'

    def __init__(self, manager):
        self.manager = manager
        self.logger = logging.getLogger("hbase.backend")
        settings = manager.settings
        port = settings.get('HBASE_THRIFT_PORT')
        hosts = settings.get('HBASE_THRIFT_HOST')
        namespace = settings.get('HBASE_NAMESPACE')
        self._min_requests = settings.get('BC_MIN_REQUESTS')
        self._min_hosts = settings.get('BC_MIN_HOSTS')
        self._max_requests_per_host = settings.get('BC_MAX_REQUESTS_PER_HOST')

        self.queue_partitions = settings.get('SPIDER_FEED_PARTITIONS')
        host = choice(hosts) if type(hosts) in [list, tuple] else hosts
        kwargs = {
            'host': host,
            'port': int(port),
            'table_prefix': namespace,
            'table_prefix_separator': ':',
            'timeout': 60000
        }
        if settings.get('HBASE_USE_FRAMED_COMPACT'):
            kwargs.update({
                'protocol': 'compact',
                'transport': 'framed'
            })
        self.logger.info("Connecting to %s:%d thrift server.", host, port)
        self.connection = Connection(**kwargs)
        self._metadata = None
        self._queue = None
        self._states = None
        self._domain_metadata = None

    def _init_states(self, settings):
        self._states = HBaseState(connection=self.connection,
                                  table_name=settings.get('HBASE_STATES_TABLE'),
                                  cache_size_limit=settings.get('HBASE_STATE_CACHE_SIZE_LIMIT'),
                                  write_log_size=settings.get('HBASE_STATE_WRITE_LOG_SIZE'),
                                  drop_all_tables=settings.get('HBASE_DROP_ALL_TABLES'))

    def _init_queue(self, settings):
        self._queue = HBaseQueue(self.connection, self.queue_partitions,
                                 settings.get('HBASE_QUEUE_TABLE'), drop=settings.get('HBASE_DROP_ALL_TABLES'),
                                 use_snappy=settings.get('HBASE_USE_SNAPPY'))

    def _init_metadata(self, settings):
        self._metadata = HBaseMetadata(self.connection, settings.get('HBASE_METADATA_TABLE'),
                                       settings.get('HBASE_DROP_ALL_TABLES'),
                                       settings.get('HBASE_USE_SNAPPY'),
                                       settings.get('HBASE_BATCH_SIZE'),
                                       settings.get('STORE_CONTENT'))

    def _init_domain_metadata(self, settings):
        self._domain_metadata = DomainCache(settings.get('HBASE_DOMAIN_METADATA_CACHE_SIZE'), self.connection,
                                            settings.get('HBASE_DOMAIN_METADATA_TABLE'),
                                            batch_size=settings.get('HBASE_DOMAIN_METADATA_BATCH_SIZE'))

    @classmethod
    def strategy_worker(cls, manager):
        o = cls(manager)
        o._init_states(manager.settings)
        o._init_domain_metadata(manager.settings)
        return o

    @classmethod
    def db_worker(cls, manager):
        o = cls(manager)
        o._init_queue(manager.settings)
        o._init_metadata(manager.settings)
        return o

    @classmethod
    def local(cls, manager):
        o = cls(manager)
        o._init_queue(manager.settings)
        o._init_states(manager.settings)
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

    @property
    def domain_metadata(self):
        return self._domain_metadata

    def frontier_start(self):
        for component in [self.metadata, self.queue, self.states, self.domain_metadata]:
            if component:
                component.frontier_start()

    def frontier_stop(self):
        for component in [self.metadata, self.queue, self.states, self.domain_metadata]:
            if component:
                component.frontier_stop()
        self.connection.close()

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
        self.logger.debug("Querying queue table.")
        results = []
        for partition_id in set(kwargs.pop('partitions', [i for i in range(self.queue_partitions)])):
            requests = self.queue.get_next_requests(
                max_next_requests, partition_id,
                min_requests=self._min_requests,
                min_hosts=self._min_hosts,
                max_requests_per_host=self._max_requests_per_host)
            results.extend(requests)
            self.logger.debug("Got %d requests for partition id %d", len(requests), partition_id)
        return results

    def get_stats(self):
        """Helper to get stats dictionary for the backend.

        For now it provides only HBase client stats.
        """
        stats = {}
        with time_elapsed('Call HBase backend get_stats()'):
            stats.update(self.connection.client.get_stats())
        if self._states:
            stats.update(self._states.get_stats())
        return stats
