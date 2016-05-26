# -*- coding: utf-8 -*-
from struct import pack, unpack
from datetime import datetime
from calendar import timegm
from time import time
from binascii import hexlify, unhexlify
from io import BytesIO
from random import choice

from happybase import Connection
from frontera.utils.url import parse_domain_from_url_fast
from msgpack import Unpacker, Packer

from frontera import DistributedBackend
from frontera.core.components import Metadata, Queue, States
from frontera.core.models import Request
from frontera.contrib.backends.partitioners import Crc32NamePartitioner
from frontera.utils.misc import chunks, get_crc32
import logging


_pack_functions = {
    'url': str,
    'depth': lambda x: pack('>I', 0),
    'created_at': lambda x: pack('>Q', x),
    'status_code': lambda x: pack('>H', x),
    'state': lambda x: pack('>B', x),
    'error': str,
    'domain_fingerprint': str,
    'score': lambda x: pack('>f', x),
    'content': str
}


def unpack_score(blob):
    return unpack(">d", blob)[0]


def prepare_hbase_object(obj=None, **kwargs):
    if not obj:
        obj = dict()
    for k, v in kwargs.iteritems():
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


class HBaseQueue(Queue):

    GET_RETRIES = 3

    def __init__(self, connection, partitions, table_name, drop=False):
        self.connection = connection
        self.partitions = [i for i in range(0, partitions)]
        self.partitioner = Crc32NamePartitioner(self.partitions)
        self.logger = logging.getLogger("hbase.queue")
        self.table_name = table_name

        tables = set(self.connection.tables())
        if drop and self.table_name in tables:
            self.connection.delete_table(self.table_name, disable=True)
            tables.remove(self.table_name)

        if self.table_name not in tables:
            self.connection.create_table(self.table_name, {'f': {'max_versions': 1, 'block_cache_enabled': 1}})

    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass

    def schedule(self, batch):
        to_schedule = []
        for fprint, score, request, schedule in batch:
            if schedule:
                if 'domain' not in request.meta:
                    _, hostname, _, _, _, _ = parse_domain_from_url_fast(request.url)
                    if not hostname:
                        self.logger.error("Can't get hostname for URL %s, fingerprint %s", request.url, fprint)
                    request.meta['domain'] = {'name': hostname}
                to_schedule.append((score, fprint, request.meta['domain'], request.url))
        self._schedule(to_schedule)

    def _schedule(self, batch):
        """
        Row - portion of the queue for each partition id created at some point in time
        Row Key - partition id + score interval + timestamp
        Column Qualifier - discrete score (first three digits after dot, e.g. 0.001_0.002, 0.002_0.003, ...)
        Value - QueueCell msgpack blob

        Where score is mapped from 0.0 to 1.0
        score intervals are
          [0.01-0.02)
          [0.02-0.03)
          [0.03-0.04)
         ...
          [0.99-1.00]
        timestamp - the time when links was scheduled for retrieval.

        :param batch: list of tuples(score, fingerprint, domain, url)
        :return:
        """
        def get_interval(score, resolution):
            if score < 0.0 or score > 1.0:
                raise OverflowError

            i = int(score / resolution)
            if i % 10 == 0 and i > 0:
                i = i - 1  # last interval is inclusive from right
            return (i * resolution, (i + 1) * resolution)

        timestamp = int(time() * 1E+6)
        data = dict()
        for score, fingerprint, domain, url in batch:
            if type(domain) == dict:
                partition_id = self.partitioner.partition(domain['name'], self.partitions)
                host_crc32 = get_crc32(domain['name'])
            elif type(domain) == int:
                partition_id = self.partitioner.partition_by_hash(domain, self.partitions)
                host_crc32 = domain
            else:
                raise TypeError("domain of unknown type.")
            item = (unhexlify(fingerprint), host_crc32, url, score)
            score = 1 - score  # because of lexicographical sort in HBase
            rk = "%d_%s_%d" % (partition_id, "%0.2f_%0.2f" % get_interval(score, 0.01), timestamp)
            data.setdefault(rk, []).append((score, item))

        table = self.connection.table(self.table_name)
        with table.batch(transaction=True) as b:
            for rk, tuples in data.iteritems():
                obj = dict()
                for score, item in tuples:
                    column = 'f:%0.3f_%0.3f' % get_interval(score, 0.001)
                    obj.setdefault(column, []).append(item)

                final = dict()
                packer = Packer()
                for column, items in obj.iteritems():
                    stream = BytesIO()
                    for item in items:
                        stream.write(packer.pack(item))
                    final[column] = stream.getvalue()
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
        min_hosts = kwargs.pop('min_hosts')
        max_requests_per_host = kwargs.pop('max_requests_per_host')
        assert(max_n_requests > min_requests)
        table = self.connection.table(self.table_name)

        meta_map = {}
        queue = {}
        limit = min_requests
        tries = 0
        count = 0
        while tries < self.GET_RETRIES:
            tries += 1
            limit *= 5.5 if tries > 1 else 1.0
            self.logger.debug("Try %d, limit %d, last attempt: requests %d, hosts %d",
                              tries, limit, count, len(queue.keys()))
            meta_map.clear()
            queue.clear()
            count = 0
            for rk, data in table.scan(row_prefix='%d_' % partition_id, limit=int(limit), batch_size=256):
                for cq, buf in data.iteritems():
                    stream = BytesIO(buf)
                    unpacker = Unpacker(stream)
                    for item in unpacker:
                        fingerprint, host_crc32, url, score = item
                        if host_crc32 not in queue:
                            queue[host_crc32] = []
                        if max_requests_per_host is not None and len(queue[host_crc32]) > max_requests_per_host:
                            continue
                        queue[host_crc32].append(fingerprint)
                        count += 1

                        if fingerprint not in meta_map:
                            meta_map[fingerprint] = []
                        meta_map[fingerprint].append((rk, item))
                if count > max_n_requests:
                    break

            if min_hosts is not None and len(queue.keys()) < min_hosts:
                continue

            if count < min_requests:
                continue
            break

        self.logger.debug("Finished: tries %d, hosts %d, requests %d", tries, len(queue.keys()), count)

        # For every fingerprint collect it's row keys and return all fingerprints from them
        fprint_map = {}
        for fprint, meta_list in meta_map.iteritems():
            for rk, _ in meta_list:
                fprint_map.setdefault(rk, []).append(fprint)

        results = []
        trash_can = set()

        for _, fprints in queue.iteritems():
            for fprint in fprints:
                for rk, _ in meta_map[fprint]:
                    if rk in trash_can:
                        continue
                    for rk_fprint in fprint_map[rk]:
                        _, item = meta_map[rk_fprint][0]
                        _, _, url, score = item
                        results.append(Request(url, meta={
                            'fingerprint': hexlify(rk_fprint),
                            'score': score,
                        }))
                    trash_can.add(rk)

        with table.batch(transaction=True) as b:
            for rk in trash_can:
                b.delete(rk)
        self.logger.debug("%d row keys removed", len(trash_can))
        return results

    def count(self):
        return NotImplementedError


class HBaseState(States):

    def __init__(self, connection, table_name, cache_size_limit):
        self.connection = connection
        self._table_name = table_name
        self.logger = logging.getLogger("hbase.states")
        self._state_cache = {}
        self._cache_size_limit = cache_size_limit

    def update_cache(self, objs):
        objs = objs if type(objs) in [list, tuple] else [objs]

        def put(obj):
            self._state_cache[obj.meta['fingerprint']] = obj.meta['state']
        map(put, objs)

    def set_states(self, objs):
        objs = objs if type(objs) in [list, tuple] else [objs]

        def get(obj):
            fprint = obj.meta['fingerprint']
            obj.meta['state'] = self._state_cache[fprint] if fprint in self._state_cache else States.DEFAULT
        map(get, objs)

    def flush(self, force_clear):
        if len(self._state_cache) > self._cache_size_limit:
            force_clear = True
        table = self.connection.table(self._table_name)
        for chunk in chunks(self._state_cache.items(), 32768):
            with table.batch(transaction=True) as b:
                for fprint, state in chunk:
                    hb_obj = prepare_hbase_object(state=state)
                    b.put(unhexlify(fprint), hb_obj)
        if force_clear:
            self.logger.debug("Cache has %d requests, clearing" % len(self._state_cache))
            self._state_cache.clear()

    def fetch(self, fingerprints):
        to_fetch = [f for f in fingerprints if f not in self._state_cache]
        self.logger.debug("cache size %s" % len(self._state_cache))
        self.logger.debug("to fetch %d from %d" % (len(to_fetch), len(fingerprints)))
        for chunk in chunks(to_fetch, 65536):
            keys = [unhexlify(fprint) for fprint in chunk]
            table = self.connection.table(self._table_name)
            records = table.rows(keys, columns=['s:state'])
            for key, cells in records:
                if 's:state' in cells:
                    state = unpack('>B', cells['s:state'])[0]
                    self._state_cache[hexlify(key)] = state


class HBaseMetadata(Metadata):
    def __init__(self, connection, table_name, drop_all_tables, use_snappy, batch_size, store_content):
        self._table_name = table_name
        tables = set(connection.tables())
        if drop_all_tables and self._table_name in tables:
            connection.delete_table(self._table_name, disable=True)
            tables.remove(self._table_name)

        if self._table_name not in tables:
            schema = {'m': {'max_versions': 1},
                      's': {'max_versions': 1, 'block_cache_enabled': 1,
                            'bloom_filter_type': 'ROW', 'in_memory': True, },
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
                                       domain_fingerprint=seed.meta['domain']['fingerprint'])
            self.batch.put(unhexlify(seed.meta['fingerprint']), obj)

    def page_crawled(self, response, links):
        obj = prepare_hbase_object(status_code=response.status_code, content=response.body) if self.store_content else \
            prepare_hbase_object(status_code=response.status_code)
        links_dict = dict()
        for link in links:
            links_dict[unhexlify(link.meta['fingerprint'])] = (link, link.url, link.meta['domain'])
        self.batch.put(unhexlify(response.meta['fingerprint']), obj)
        for link_fingerprint, (link, link_url, link_domain) in links_dict.iteritems():
            obj = prepare_hbase_object(url=link_url,
                                       created_at=utcnow_timestamp(),
                                       domain_fingerprint=link_domain['fingerprint'])
            self.batch.put(link_fingerprint, obj)

    def request_error(self, request, error):
        obj = prepare_hbase_object(url=request.url,
                                   created_at=utcnow_timestamp(),
                                   error=error,
                                   domain_fingerprint=request.meta['domain']['fingerprint'])
        rk = unhexlify(request.meta['fingerprint'])
        self.batch.put(rk, obj)

    def update_score(self, batch):
        if not isinstance(batch, dict):
            raise TypeError('batch should be dict with fingerprint as key, and float score as value')
        for fprint, (score, url, schedule) in batch.iteritems():
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
        self.queue_partitions = settings.get('SPIDER_FEED_PARTITIONS')
        host = choice(hosts) if type(hosts) in [list, tuple] else hosts
        kwargs = {
            'host': host,
            'port': int(port),
            'table_prefix': namespace,
            'table_prefix_separator': ':'
        }
        if settings.get('HBASE_USE_FRAMED_COMPACT'):
            kwargs.update({
                'protocol': 'compact',
                'transport': 'framed'
            })
        self.connection = Connection(**kwargs)
        self._metadata = None
        self._queue = None
        self._states = None

    @classmethod
    def strategy_worker(cls, manager):
        o = cls(manager)
        settings = manager.settings
        o._states = HBaseState(o.connection, settings.get('HBASE_METADATA_TABLE'),
                               settings.get('HBASE_STATE_CACHE_SIZE_LIMIT'))
        return o

    @classmethod
    def db_worker(cls, manager):
        o = cls(manager)
        settings = manager.settings
        drop_all_tables = settings.get('HBASE_DROP_ALL_TABLES')
        o._queue = HBaseQueue(o.connection, o.queue_partitions,
                              settings.get('HBASE_QUEUE_TABLE'), drop=drop_all_tables)
        o._metadata = HBaseMetadata(o.connection, settings.get('HBASE_METADATA_TABLE'), drop_all_tables,
                                    settings.get('HBASE_USE_SNAPPY'), settings.get('HBASE_BATCH_SIZE'),
                                    settings.get('STORE_CONTENT'))
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
        self.connection.close()

    def add_seeds(self, seeds):
        self.metadata.add_seeds(seeds)

    def page_crawled(self, response, links):
        self.metadata.page_crawled(response, links)

    def request_error(self, page, error):
        self.metadata.request_error(page, error)

    def finished(self):
        raise NotImplementedError

    def get_next_requests(self, max_next_requests, **kwargs):
        next_pages = []
        self.logger.debug("Querying queue table.")
        partitions = set(kwargs.pop('partitions', []))
        for partition_id in range(0, self.queue_partitions):
            if partition_id not in partitions:
                continue
            results = self.queue.get_next_requests(max_next_requests, partition_id, min_requests=64,
                                                   min_hosts=24, max_requests_per_host=128)
            next_pages.extend(results)
            self.logger.debug("Got %d requests for partition id %d", len(results), partition_id)
        return next_pages
