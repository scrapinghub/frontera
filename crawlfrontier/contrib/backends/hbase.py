# -*- coding: utf-8 -*-
from happybase import Connection
from crawlfrontier import Backend
from crawlfrontier.worker.partitioner import Crc32NamePartitioner
from crawlfrontier.utils.misc import chunks
from crawlfrontier.utils.url import parse_domain_from_url_fast

from struct import pack, unpack, unpack_from
from datetime import datetime
from calendar import timegm
from time import time
from binascii import hexlify, unhexlify
from zlib import crc32
from msgpack import Unpacker, Packer
from io import BytesIO
from random import choice



class State:
    states = {
        'NOT_CRAWLED': 0,
        'QUEUED': 1,
        'CRAWLED': 2,
        'ERROR': 3
    }

    def __init__(self):
        self.states_by_id = dict(zip(self.states.values(), self.states.keys()))

    def get_id(self, state):
        return self.states[state]

    def get_name(self, id):
        return self.states_by_id[id]


_state = State()
_pack_functions = {
    'url': str,
    'depth': lambda x: pack('>I', 0),
    'created_at': lambda x: pack('>Q', x),
    'status_code': lambda x: pack('>H', x),
    'state': lambda x: pack('>B', _state.get_id(x) if type(x) == str else x),
    'error': str,
    'domain_fingerprint': str,
    'score': lambda x: pack('>f', x)
}

def unpack_score(blob):
    return unpack(">d", blob)[0]

def prepare_hbase_object(obj=None, **kwargs):
    if not obj:
        obj = dict()
    for k, v in kwargs.iteritems():
        cf = 'm' if k not in ['score', 'state'] else 's'
        func = _pack_functions[k]
        obj[cf+':'+k] = func(v)
    return obj

def utcnow_timestamp():
    d = datetime.utcnow()
    return timegm(d.timetuple())


class HBaseQueue(object):

    GET_RETRIES = 1

    def __init__(self, connection, partitions, logger, drop=False):
        self.connection = connection
        self.partitions = [i for i in range(0, partitions)]
        self.partitioner = Crc32NamePartitioner(self.partitions)
        self.logger = logger
        self.table_name = 'new_queue_msgpack'

        tables = set(self.connection.tables())
        if drop and self.table_name in tables:
            self.connection.delete_table(self.table_name, disable=True)
            tables.remove(self.table_name)

        if self.table_name not in tables:
            self.connection.create_table(self.table_name, {'f': {'max_versions': 1, 'block_cache_enabled': 1}})

    def schedule(self, links):
        """
        Row - portion of the queue for each partition id created at some point in time
        Row Key - partition id + score interval + timestamp
        Column Qualifier - discrete score (first two digits after dot, e.g. 0.01_0.02, 0.02_0.03, ...)
        Value - QueueCell protobuf class

        Where score is mapped from 0.0 to 1.0
        score intervals are
          [0.0-0.1)
          [0.1-0.2)
          [0.2-0.3)
         ...
          [0.9-1.0]
        timestamp - the time when links was scheduled for retrieval.

        :param links:
        :return:
        """
        def get_crc32(name):
            return crc32(name) if type(name) is str else crc32(name.encode('utf-8', 'ignore'))

        def get_interval(score, resolution):
            if score < 0.0 or score > 1.0:
                raise OverflowError

            i = int(score / resolution)
            if i % 10 == 0 and i > 0:
                i = i-1  # last interval is inclusive from right
            return (i * resolution, (i+1) * resolution)

        timestamp = int(time() * 1E+6)
        data = dict()
        for score, fingerprint, domain, url in links:
            if type(domain) == dict:
                partition_id = self.partitioner.partition(domain['name'], self.partitions)
                host_crc32 = get_crc32(domain['name'])
            elif type(domain) == int:
                partition_id = self.partitioner.partition_by_hash(domain, self.partitions)
                host_crc32 = domain
            else:
                raise TypeError("domain of unknown type.")
            item = [unhexlify(fingerprint), host_crc32, url, score]
            score = 1 - score  # because of lexicographical sort in HBase
            rk = "%d_%s_%d" %(partition_id, "%0.2f_%0.2f" % get_interval(score, 0.01), timestamp)
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
                    for item in items: stream.write(packer.pack(item))
                    final[column] = stream.getvalue()
                b.put(rk, final)

    def get(self, partition_id, min_requests, min_hosts=None, max_requests_per_host=None):
        table = self.connection.table(self.table_name)

        meta_map = {}
        queue = {}
        limit = min_requests
        tries = 0
        count = 0
        while tries < self.GET_RETRIES:
            tries += 1
            limit *= 5.5 if tries > 1 else 1.0
            self.logger.debug("Try %d, limit %d, last attempt: requests %d, hosts %d" % (tries, limit, count, len(queue.keys())))
            meta_map.clear()
            queue.clear()
            for rk, data in table.scan(row_prefix='%d_' % partition_id, limit=int(limit)):
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

                        if fingerprint not in meta_map:
                            meta_map[fingerprint] = []
                        meta_map[fingerprint].append((rk, item))

            count = 0
            for host_id, fprints in queue.iteritems():
                count += len(fprints)

            if min_hosts is not None and len(queue.keys()) < min_hosts:
                continue

            if count < min_requests:
                continue
            break

        self.logger.debug("Tries %d, hosts %d, requests %d" % (tries, len(queue.keys()), count))

        # For every fingerprint collect it's row keys and return all fingerprints from them
        fprint_map = {}
        for fprint, meta_list in meta_map.iteritems():
            for rk, _ in meta_list:
                fprint_map.setdefault(rk, []).append(fprint)

        results = set()
        trash_can = set()
        for _, fprints in queue.iteritems():
            for fprint in fprints:
                for rk, _ in meta_map[fprint]:
                    trash_can.add(rk)
                    for rk_fprint in fprint_map[rk]:
                        _, item = meta_map[rk_fprint][0]
                        _, _, url, score = item
                        results.add((hexlify(rk_fprint), url, score))

        with table.batch(transaction=True) as b:
            for rk in trash_can:
                b.delete(rk)
        self.logger.debug("%d row keys removed" % (len(trash_can)))
        return results

    def rebuild(self, table_name):
        pass


class HBaseState(object):

    def __init__(self, connection, table_name):
        self.connection = connection
        self._table_name = table_name
        self._state_cache = {}

    def update(self, objs, persist):
        objs = objs if type(objs) in [list, tuple] else [objs]
        if persist:
            def put(obj):
                if obj.meta['state'] is not None:
                    self._state_cache[obj.meta['fingerprint']] = obj.meta['state']
            map(put, objs)
            return

        def get(obj):
            fprint = obj.meta['fingerprint']
            obj.meta['state'] = self._state_cache[fprint] if fprint in self._state_cache else None
        map(get, objs)

    def flush(self, is_clear):
        table = self.connection.table(self._table_name)
        with table.batch(transaction=True) as b:
            for fprint, state in self._state_cache.iteritems():
                hb_obj = prepare_hbase_object(state=state)
                b.put(unhexlify(fprint), hb_obj)
        if is_clear:
            self._state_cache.clear()

    def fetch(self, fingerprints):
        to_fetch = [f for f in fingerprints if f not in self._state_cache]
        print "to fetch %d from %d" % (len(to_fetch), len(fingerprints))
        for chunk in chunks(to_fetch, 131072):
            keys = [unhexlify(fprint) for fprint in chunk]
            table = self.connection.table(self._table_name)
            records = table.rows(keys, columns=['s:state'])
            for key, cells in records:
                if 's:state' in cells:
                    state = unpack('>B', cells['s:state'])[0]
                    self._state_cache[hexlify(key)] = state


class HBaseBackend(Backend):
    component_name = 'HBase Backend'

    def __init__(self, manager):
        self.manager = manager

        settings = manager.settings
        port = settings.get('HBASE_THRIFT_PORT', 9090)
        hosts = settings.get('HBASE_THRIFT_HOST', 'localhost')
        namespace = settings.get('HBASE_NAMESPACE', 'crawler')
        drop_all_tables = settings.get('HBASE_DROP_ALL_TABLES', False)
        self.queue_partitions = settings.get('HBASE_QUEUE_PARTITIONS', 4)
        self._table_name = settings.get('HBASE_METADATA_TABLE', 'metadata')
        host = choice(hosts) if type(hosts) in [list, tuple] else hosts

        self.connection = Connection(host=host, port=int(port), table_prefix=namespace, table_prefix_separator=':')
        self.queue = HBaseQueue(self.connection, self.queue_partitions, self.manager.logger.backend,
                                drop=drop_all_tables)
        self.state_checker = HBaseState(self.connection, self._table_name)


        tables = set(self.connection.tables())
        if drop_all_tables and self._table_name in tables:
            self.connection.delete_table(self._table_name, disable=True)
            tables.remove(self._table_name)

        if self._table_name not in tables:
            self.connection.create_table(self._table_name, {'m': {'max_versions': 5}, # 'compression': 'SNAPPY'
                                                            's': {'max_versions': 1, 'block_cache_enabled': 1,
                                                            'bloom_filter_type': 'ROW', 'in_memory': True, }
                                                            })
        table = self.connection.table(self._table_name)
        self.batch = table.batch(batch_size=12288)

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def frontier_start(self):
        pass

    def frontier_stop(self):
        self.connection.close()
        self.flush()

    def add_seeds(self, seeds):
        for seed in seeds:
            url, fingerprint, domain = self.manager.canonicalsolver.get_canonical_url(seed)
            obj = prepare_hbase_object(url=url,
                                       depth=0,
                                       created_at=utcnow_timestamp(),
                                       domain_fingerprint=domain['fingerprint'])
            self.batch.put(unhexlify(fingerprint), obj)

    def page_crawled(self, response, links):
        url, fingerprint, domain = self.manager.canonicalsolver.get_canonical_url(response)
        obj = prepare_hbase_object(status_code=response.status_code)

        links_dict = dict()
        for link in links:
            link_url, link_fingerprint, link_domain = self.manager.canonicalsolver.get_canonical_url(link)
            links_dict[unhexlify(link_fingerprint)] = (link, link_url, link_domain)


        self.batch.put(unhexlify(fingerprint), obj)
        for link_fingerprint, (link, link_url, link_domain) in links_dict.iteritems():
            obj = prepare_hbase_object(url=link_url,
                                       created_at=utcnow_timestamp(),
                                       domain_fingerprint=link_domain['fingerprint'])
            self.batch.put(link_fingerprint, obj)

    def request_error(self, request, error):
        url, fingerprint, domain = self.manager.canonicalsolver.get_canonical_url(request)
        obj = prepare_hbase_object(url=request.url,
                                   created_at=utcnow_timestamp(),
                                   error=error,
                                   domain_fingerprint=domain['fingerprint'])
        rk = unhexlify(request.meta['fingerprint'])
        self.batch.put(rk, obj)

    def get_next_requests(self, max_next_requests, **kwargs):
        next_pages = []
        log = self.manager.logger.backend
        log.debug("Querying queue table.")
        partitions = set(kwargs.pop('partitions', []))
        for partition_id in range(0, self.queue_partitions):
            if partition_id not in partitions:
                continue
            results = self.queue.get(partition_id, max_next_requests,
                                                    min_hosts=24, max_requests_per_host=128)

            log.debug("Got %d items for partition id %d" % (len(results), partition_id))
            for fingerprint, url, score in results:
                r = self.manager.request_model(url=url)
                r.meta['fingerprint'] = fingerprint
                r.meta['score'] = score
                next_pages.append(r)
        return next_pages

    def update_score(self, batch):
        if not isinstance(batch, dict):
            raise TypeError('batch should be dict with fingerprint as key, and float score as value')

        to_schedule = []
        for fprint, (score, url, schedule) in batch.iteritems():
            obj = prepare_hbase_object(score=score)
            rk = unhexlify(fprint)
            self.batch.put(rk, obj)
            if schedule:
                _, hostname, _, _, _, _ = parse_domain_from_url_fast(url)
                if not hostname:
                    self.manager.logger.backend.error("Can't get hostname for URL %s, fingerprint %s" % (url, fprint))
                    continue
                to_schedule.append((score, fprint, {'name': hostname}, url))
        self.queue.schedule(to_schedule)

    def flush(self):
        self.batch.send()

    def update_states(self, objs, persist):
        self.state_checker.update(objs, persist)

    def flush_states(self, is_clear=True):
        self.state_checker.flush(is_clear)

    def fetch_states(self, fingerprints):
        self.state_checker.fetch(fingerprints)

