from __future__ import absolute_import

import logging

from binascii import unhexlify
from io import BytesIO
from time import time

from msgpack import Packer, Unpacker

import six

from w3lib.util import to_bytes

from frontera.contrib.backends.partitioners import Crc32NamePartitioner
from frontera.contrib.backends.remote.codecs.msgpack import Decoder, Encoder
from frontera.core.components import Queue
from frontera.core.models import Request
from frontera.utils.misc import get_crc32
from frontera.utils.url import parse_domain_from_url_fast


logger = logging.getLogger(__name__)


class HBaseQueue(Queue):

    GET_RETRIES = 3

    def __init__(self, connection, partitions, table_name, drop=False):
        self.connection = connection
        self.partitions = [i for i in range(0, partitions)]
        self.partitioner = Crc32NamePartitioner(self.partitions)
        self.table_name = to_bytes(table_name)

        tables = set(self.connection.tables())

        if drop and self.table_name in tables:
            self.connection.delete_table(self.table_name, disable=True)
            tables.remove(self.table_name)

        if self.table_name not in tables:
            self.connection.create_table(
                self.table_name,
                {
                    'f': {
                        'max_versions': 1,
                        'block_cache_enabled': 1,
                    },
                },
            )

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
                if b'domain' not in request.meta:
                    # TODO: this have to be done always by DomainMiddleware,
                    #       so I propose to require DomainMiddleware by
                    #       HBaseBackend and remove that code
                    _, hostname, _, _, _, _ = parse_domain_from_url_fast(request.url)

                    if not hostname:
                        logger.error(
                            "Can't get hostname for URL %s, fingerprint %s",
                            request.url, fprint,
                        )

                    request.meta[b'domain'] = {'name': hostname}

                if b'crawl_at' in request.meta:
                    timestamp = request.meta[b'crawl_at']
                else:
                    timestamp = now

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

            if type(domain) == dict:
                partition_id = self.partitioner.partition(domain[b'name'], self.partitions)
                host_crc32 = get_crc32(domain[b'name'])
            elif type(domain) == int:
                partition_id = self.partitioner.partition_by_hash(domain, self.partitions)
                host_crc32 = domain
            else:
                raise TypeError("domain of unknown type.")

            item = (unhexlify(fingerprint), host_crc32, self.encoder.encode_request(request), score)
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
        min_hosts = kwargs.pop('min_hosts')
        max_requests_per_host = kwargs.pop('max_requests_per_host')

        assert max_n_requests > min_requests

        table = self.connection.table(self.table_name)

        meta_map = {}
        queue = {}
        limit = min_requests
        tries = 0
        count = 0
        prefix = '%d_' % partition_id
        now_ts = int(time())
        filter = "PrefixFilter ('%s') AND SingleColumnValueFilter ('f', 't', <=, 'binary:%d')" % (prefix, now_ts)

        while tries < self.GET_RETRIES:
            tries += 1
            limit *= 5.5 if tries > 1 else 1.0

            logger.debug(
                "Try %d, limit %d, last attempt: requests %d, hosts %d",
                tries, limit, count, len(queue.keys()),
            )
            meta_map.clear()
            queue.clear()

            count = 0

            for rk, data in table.scan(limit=int(limit), batch_size=256, filter=filter):
                for cq, buf in six.iteritems(data):
                    if cq == b'f:t':
                        continue

                    stream = BytesIO(buf)
                    unpacker = Unpacker(stream)

                    for item in unpacker:
                        fprint, host_crc32, _, _ = item

                        if host_crc32 not in queue:
                            queue[host_crc32] = []

                        if max_requests_per_host is not None:
                            if len(queue[host_crc32]) > max_requests_per_host:
                                continue

                        queue[host_crc32].append(fprint)

                        count += 1

                        if fprint not in meta_map:
                            meta_map[fprint] = []

                        meta_map[fprint].append((rk, item))

                if count > max_n_requests:
                    break

            if min_hosts is not None and len(queue.keys()) < min_hosts:
                continue

            if count < min_requests:
                continue

            break

        logger.debug(
            "Finished: tries %d, hosts %d, requests %d", tries,
            len(queue.keys()), count,
        )

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

        logger.debug("%d row keys removed", len(trash_can))

        return results

    def count(self):
        raise NotImplementedError
