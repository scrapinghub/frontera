from __future__ import absolute_import

import logging
import random

import six

from six.moves import range

from frontera.contrib.backends.partitioners import Crc32NamePartitioner
from frontera.core.components import Queue
from frontera.utils.heap import Heap
from frontera.utils.url import parse_domain_from_url_fast

from .utils import cmp


logger = logging.getLogger(__name__)


class MemoryQueue(Queue):
    def __init__(self, partitions):
        self.partitions = [i for i in range(0, partitions)]
        self.partitioner = Crc32NamePartitioner(self.partitions)
        self.heap = {
            partition: Heap(self._compare_pages)
            for partition in self.partitions
        }

    def count(self):
        return sum(len(h.heap) for h in six.itervalues(self.heap))

    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        return self.heap[partition_id].pop(max_n_requests)

    def schedule(self, batch):
        for fprint, score, request, schedule in batch:
            if schedule:
                request.meta[b'_scr'] = score
                _, hostname, _, _, _, _ = parse_domain_from_url_fast(request.url)

                if hostname:
                    partition_id = self.partitioner.partition(hostname, self.partitions)
                else:
                    logger.error(
                        "Can't get hostname for URL %s, fingerprint %s",
                        request.url, fprint,
                    )

                    partition_id = self.partitions[0]

                self.heap[partition_id].push(request)

    def _compare_pages(self, first, second):
        return cmp(first.meta[b'_scr'], second.meta[b'_scr'])


class MemoryDFSQueue(MemoryQueue):
    def _compare_pages(self, first, second):
        return cmp(
            (second.meta[b'depth'], first.meta[b'id']),
            (first.meta[b'depth'], second.meta[b'id']),
        )


class MemoryBFSQueue(MemoryQueue):
    def _compare_pages(self, first, second):
        return cmp(
            (first.meta[b'depth'], first.meta[b'id']),
            (second.meta[b'depth'], second.meta[b'id']),
        )


class MemoryRandomQueue(MemoryQueue):
    def _compare_pages(self, first, second):
        return random.choice([-1, 0, 1])
