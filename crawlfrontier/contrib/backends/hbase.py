# -*- coding: utf-8 -*-
from happybase import Connection
from crawlfrontier import Backend
from crawlfrontier.worker.partitioner import Crc32NamePartitioner
from crawlfrontier.utils.misc import chunks
from crawlfrontier.utils.url import parse_domain_from_url_fast
from hbase_queue_pb2 import QueueCell, QueueItem

from struct import pack, unpack, unpack_from
from datetime import datetime
from calendar import timegm
from time import time
from binascii import hexlify, unhexlify
from zlib import crc32



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
    'state': lambda x: pack('>B', _state.get_id(x)),
    'error': str,
    'domain_fingerprint': str,
    'score': lambda x: pack('>d', x)
}

def unpack_score(blob):
    return unpack(">d", blob)[0]

def prepare_hbase_object(obj=None, **kwargs):
    if not obj:
        obj = dict()
    for k, v in kwargs.iteritems():
        cf = 'm' if k != 'score' else 's'
        func = _pack_functions[k]
        obj[cf+':'+k] = func(v)
    return obj

def utcnow_timestamp():
    d = datetime.utcnow()
    return timegm(d.timetuple())


class HBaseQueue(object):

    GET_RETRIES = 3

    def __init__(self, connection, partitions, logger, drop=False):
        self.connection = connection
        self.partitions = [i for i in range(0, partitions)]
        self.partitioner = Crc32NamePartitioner(self.partitions)
        self.logger = logger

        tables = set(self.connection.tables())
        if drop and 'queue' in tables:
            self.connection.delete_table('queue', disable=True)
            tables.remove('queue')

        if 'queue' not in tables:
            self.connection.create_table('queue', {'f': {'max_versions': 1, 'block_cache_enabled': 1}})

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
            item = QueueItem()
            item.fingerprint = fingerprint
            item.url = url
            item.score = score
            if type(domain) == dict:
                partition_id = self.partitioner.partition(domain['name'], self.partitions)
                item.host_crc32 = get_crc32(domain['name'])
            elif type(domain) == int:
                partition_id = self.partitioner.partition_by_hash(domain, self.partitions)
                item.host_crc32 = domain
            else:
                raise TypeError("domain of unknown type.")
            score = 1 - score  # because of lexicographical sort in HBase
            rk = "%d_%s_%d" %(partition_id, "%0.2f_%0.2f" % get_interval(score, 0.01), timestamp)
            data.setdefault(rk, []).append((score, item))

        table = self.connection.table('queue')
        with table.batch(transaction=True) as b:
            for rk, tuples in data.iteritems():
                obj = dict()
                for score, item in tuples:
                    column = 'f:%0.3f_%0.3f' % get_interval(score, 0.001)
                    obj.setdefault(column, []).append(item)

                final = dict()
                for column, items in obj.iteritems():
                    cell = QueueCell()
                    cell.items.extend(items)
                    final[column] = cell.SerializeToString()
                b.put(rk, final)

    def get(self, partition_id, min_requests, min_hosts=None, max_requests_per_host=None):
        table = self.connection.table('queue')

        meta_map = {}
        queue = {}
        limit = min_requests
        tries = 0
        count = 0
        while tries < self.GET_RETRIES:
            tries += 1
            limit *= 5.5 if tries > 1 else 1.0
            self.logger.debug("Try %d, limit %d, requests %d, hosts %d" % (tries, limit, count, len(queue.keys())))
            meta_map.clear()
            queue.clear()
            for rk, data in table.scan(row_prefix='%d_' % partition_id, limit=int(limit)):
                for cq, buf in data.iteritems():
                    cell = QueueCell.FromString(buf)
                    for item in cell.items:
                        if item.host_crc32 not in queue:
                            queue[item.host_crc32] = []
                        if max_requests_per_host is not None and len(queue[item.host_crc32]) > max_requests_per_host:
                            continue
                        queue[item.host_crc32].append(item.fingerprint)

                        if item.fingerprint not in meta_map:
                            meta_map[item.fingerprint] = []
                        meta_map[item.fingerprint].append((rk, item))

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
                        results.add((hexlify(rk_fprint), item.url, item.score))

        with table.batch(transaction=True) as b:
            for rk in trash_can:
                b.delete(rk)
        self.logger.debug("%d row keys removed" % (len(trash_can)))
        return results

    def rebuild(self, table_name):
        pass


class HBaseBackend(Backend):
    component_name = 'HBase Backend'

    def __init__(self, manager):
        self.manager = manager

        settings = manager.settings
        port = settings.get('HBASE_THRIFT_PORT', 9090)
        host = settings.get('HBASE_THRIFT_HOST', 'localhost')
        namespace = settings.get('HBASE_NAMESPACE', 'crawler')
        drop_all_tables = settings.get('HBASE_DROP_ALL_TABLES', False)
        self.queue_partitions = settings.get('HBASE_QUEUE_PARTITIONS', 4)

        self.connection = Connection(host=host, port=int(port), table_prefix=namespace, table_prefix_separator=':')
        self.queue = HBaseQueue(self.connection, self.queue_partitions, self.manager.logger.backend,
                                drop=drop_all_tables)

        tables = set(self.connection.tables())
        if drop_all_tables and 'metadata' in tables:
            self.connection.delete_table('metadata', disable=True)
            tables.remove('metadata')

        if 'metadata' not in tables:
            self.connection.create_table('metadata', {'m': {'max_versions': 1, 'block_cache_enabled': 1,
                                                            'bloom_filter_type': 'ROW'},
                                                      's': {'max_versions': 1}
                                                     })

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def frontier_start(self):
        pass

    def frontier_stop(self):
        self.connection.close()

    def add_seeds(self, seeds):
        table = self.connection.table('metadata')
        with table.batch(transaction=True) as b:
            for seed in seeds:
                url, fingerprint, domain = self.manager.canonicalsolver.get_canonical_url(seed)
                obj = prepare_hbase_object(url=url,
                                           depth=0,
                                           created_at=utcnow_timestamp(),
                                           state='NOT_CRAWLED',
                                           domain_fingerprint=domain['fingerprint'])

                b.put(fingerprint, obj)

    def page_crawled(self, response, links):
        table = self.connection.table('metadata')
        url, fingerprint, domain = self.manager.canonicalsolver.get_canonical_url(response)
        record = table.row(fingerprint, columns=['m:depth'])
        if not record:
            self.manager.logger.backend.error('Unseen record in page_crawled(), url=%s, fingerprint=%s' % (url,
                                              fingerprint))
            depth = 0
            # FIXME put the rest of metadata in obj
        else:
            depth, = unpack('>I', record['m:depth'])

        obj = prepare_hbase_object(state='CRAWLED',
                                   status_code=response.status_code)

        links_dict = dict()
        for link in links:
            link_url, link_fingerprint, link_domain = self.manager.canonicalsolver.get_canonical_url(link)
            links_dict[link_fingerprint] = (link, link_url, link_domain)

        links_metadata = dict(table.rows(links_dict.keys(), columns=['m:state']))
        with table.batch(transaction=True) as b:
            b.put(fingerprint, obj)
            for link_fingerprint, (link, link_url, link_domain) in links_dict.iteritems():
                if link_fingerprint not in links_metadata:
                    obj = prepare_hbase_object(url=link_url,
                                               depth=depth+1,
                                               created_at=utcnow_timestamp(),
                                               state='NOT_CRAWLED',
                                               domain_fingerprint=link_domain['fingerprint'])
                    b.put(link_fingerprint, obj)

    def request_error(self, request, error):
        table = self.connection.table('metadata')
        url, fingerprint, domain = self.manager.canonicalsolver.get_canonical_url(request)
        record = table.row(fingerprint, columns=['m:state'])
        obj = prepare_hbase_object(state='ERROR',
                                   error=error)
        if not record:
            prepare_hbase_object(obj, url=request.url,
                                      depth=0,
                                      created_at=utcnow_timestamp(),
                                      domain_fingerprint=domain['fingerprint'])

        table.put(request.meta['fingerprint'], obj)

    def get_next_requests(self, max_next_requests, **kwargs):
        next_pages = []
        log = self.manager.logger.backend
        log.debug("Querying queue table.")
        partitions = set(kwargs.pop('partitions', []))
        for partition_id in range(0, self.queue_partitions):
            if partition_id not in partitions:
                continue
            fingerprints = self.queue.get(partition_id, max_next_requests,
                                                    min_hosts=24, max_requests_per_host=128)

            log.debug("Got %d items for partition id %d" % (len(fingerprints), partition_id))
            for fingerprint, url, score in fingerprints:
                r = self.manager.request_model(url=url)
                r.meta['fingerprint'] = fingerprint
                r.meta['score'] = score
                next_pages.append(r)
        return next_pages

    def update_score(self, batch):
        if not isinstance(batch, dict):
            raise TypeError('batch should be dict with fingerprint as key, and float score as value')

        table = self.connection.table('metadata')
        to_schedule = []
        with table.batch(transaction=True) as b:
            for fprint, (score, url, schedule) in batch.iteritems():
                obj = prepare_hbase_object(score=score)
                b.put(fprint, obj)
                if schedule:
                    _, hostname, _, _, _, _ = parse_domain_from_url_fast(url)
                    if not hostname:
                        self.manager.logger.backend.error("Can't get hostname for URL %s, fingerprint %s" % (url, fprint))
                        continue
                    to_schedule.append((score, fprint, {'name': hostname}, url))
        self.queue.schedule(to_schedule)

