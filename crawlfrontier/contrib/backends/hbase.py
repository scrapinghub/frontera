# -*- coding: utf-8 -*-
from happybase import Connection
from crawlfrontier import Backend
from crawlfrontier.worker.partitioner import Crc32NamePartitioner

from struct import pack, unpack
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
    def __init__(self, connection, partitions, drop=False):
        self.connection = connection
        self.partitions = [i for i in range(0, partitions)]
        self.partitioner = Crc32NamePartitioner(self.partitions)

        tables = set(self.connection.tables())
        if drop and 'queue' in tables:
            self.connection.delete_table('queue', disable=True)
            tables.remove('queue')

        if 'queue' not in tables:
            self.connection.create_table('queue', {'f': {'max_versions': 1}})

    def schedule(self, links):
        """
        Row - portion of the queue for each partition id created at some point in time
        Row Key - partition id + score interval + timestamp
        Column Qualifier - discrete score (first two digits after dot, e.g. 0.01_0.02, 0.02_0.03, ...)
        Value - first four bytes uint containing count of items,
        then 24-byte sha1 fingerprint+crc32(domain name) blocks without delimiters

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
        for link, fingerprint, domain in links:
            partition_id = self.partitioner.partition(domain['name'], self.partitions)
            score = 1 - link.meta['score']  # because of lexicographical sort in HBase
            host_crc32 = get_crc32(domain['name'])
            rk = "%d_%s_%d" %(partition_id, "%0.2f_%0.2f" % get_interval(score, 0.01), timestamp)
            blob = unhexlify(fingerprint) + pack(">i", host_crc32)
            data.setdefault(rk, []).append((score, blob))

        table = self.connection.table('queue')
        with table.batch(transaction=True) as b:
            for rk, tuples in data.iteritems():
                obj = dict()
                for score, blob in tuples:
                    column = 'f:%0.3f_%0.3f' % get_interval(score, 0.001)
                    obj.setdefault(column, []).append(blob)

                final = dict()
                for column, blobs in obj.iteritems():
                    buf = pack(">I", len(blobs))
                    buf += str().join(blobs)
                    final[column] = buf
                b.put(rk, final)

    def get(self, partition_id, min_requests, min_hosts=None, max_requests_per_host=None):
        def parse_interval_left(cq):
            begin = cq.find(':')
            end = cq.find('.', begin+1)
            if begin == -1 or end == -1:
                raise TypeError('Can\'t parse \'queue\' table column qualifier.')
            return float(cq[begin+1:end+4])

        table = self.connection.table('queue')

        trash_can = set()
        queue = {}
        limit = 0
        tries = 0
        count = 0
        while tries < 10:
            tries += 1
            limit += min_requests
            trash_can.clear()
            queue.clear()
            for rk, data in table.scan(row_prefix='%d_' % partition_id, limit=limit):
                for cq, buf in data.iteritems():
                    count = unpack(">I", buf[:4])[0]
                    # interval = parse_interval_left(cq)
                    for pos in range(0, count):
                        begin = pos*24 + 4
                        end = (pos+1)*24 + 4
                        fingerprint = buf[begin:begin+20]
                        host_crc32 = unpack('>i', buf[begin+20:end])
                        queue.setdefault(host_crc32, []).append(fingerprint)
                        trash_can.add(rk)

            count = 0
            to_merge = {}
            for domain, domain_requests in queue.iteritems():
                if max_requests_per_host is not None and len(domain_requests) > max_requests_per_host:
                    to_merge[domain] = domain_requests[:max_requests_per_host]
                    count += len(to_merge[domain])
                    continue
                count += len(domain_requests)
            queue.update(to_merge)

            if min_hosts is not None and len(queue.keys()) < min_hosts:
                continue

            if count < min_requests:
                continue
            break

        with table.batch(transaction=True) as b:
            for rk in trash_can:
                b.delete(rk)

        print "Tries %d, hosts %d, requests %d" % (tries, len(queue.keys()), count)
        results = []
        for host_crc32, fingerprints in queue.iteritems():
            print "%d" % len(fingerprints)
            results.extend(map(hexlify, fingerprints))
        return results

    def rebuild(self, table_name):
        pass


def chunks(l, n):
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


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
        self.queue = HBaseQueue(self.connection, self.queue_partitions, drop=drop_all_tables)

        tables = set(self.connection.tables())
        if drop_all_tables and 'metadata' in tables:
            self.connection.delete_table('metadata', disable=True)
            tables.remove('metadata')

        if 'metadata' not in tables:
            self.connection.create_table('metadata', {'m': {'max_versions': 1},
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
            to_schedule = []
            for seed in seeds:
                url, fingerprint, domain = self.manager.canonicalsolver.get_canonical_url(seed)
                obj = prepare_hbase_object(url=url,
                                           depth=0,
                                           created_at=utcnow_timestamp(),
                                           state='NOT_CRAWLED',
                                           domain_fingerprint=domain['fingerprint'],
                                           score=seed.meta['score'])

                b.put(fingerprint, obj)
                to_schedule.append((seed, fingerprint, domain))
        self.queue.schedule(to_schedule)

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
        created = []
        with table.batch(transaction=True) as b:
            b.put(fingerprint, obj)
            for link_fingerprint, (link, link_url, link_domain) in links_dict.iteritems():
                if link_fingerprint not in links_metadata:
                    obj = prepare_hbase_object(url=link_url,
                                               depth=depth+1,
                                               created_at=utcnow_timestamp(),
                                               state='NOT_CRAWLED',
                                               domain_fingerprint=link_domain['fingerprint'],
                                               score=link.meta['score'])
                    b.put(link_fingerprint, obj)
                    created.append((link, link_fingerprint, link_domain))
        self.queue.schedule(created)

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
        fingerprints = []
        self.manager.logger.backend.debug("Querying queue table.")
        for partition_id in range(0, self.queue_partitions):
            partition_fingerprints = self.queue.get(partition_id, max_next_requests / self.queue_partitions,
                                                    min_hosts=256, max_requests_per_host=20)
            fingerprints.extend(partition_fingerprints)
            self.manager.logger.backend.debug("Got %d items for partition id %d" % (len(partition_fingerprints), partition_id))

        next_pages = []
        table = self.connection.table('metadata')
        self.manager.logger.backend.debug("Querying metadata table.")
        for chunk in chunks(fingerprints, 3000):
            self.manager.logger.backend.debug("Iterating over %d size chunk." % len(chunk))
            for rk, data in table.rows(chunk, columns=['m:url', 'm:domain_fingerprint', 's:score']):
                r = self.manager.request_model(url=data['m:url'])
                r.meta['domain'] = {
                    'fingerprint': data['m:domain_fingerprint']
                }
                r.meta['fingerprint'] = rk
                r.meta['score'] = unpack(">d", data['s:score'])[0]
                next_pages.append(r)
        self.manager.logger.backend.debug("Got %d requests." % (len(next_pages)))
        return next_pages
