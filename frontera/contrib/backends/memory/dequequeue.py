from __future__ import absolute_import

import logging

from collections import deque

import six

from six.moves import map, range

from frontera.contrib.backends.partitioners import Crc32NamePartitioner
from frontera.core.components import Queue
from frontera.utils.url import parse_domain_from_url_fast


logger = logging.getLogger(__name__)


class MemoryDequeQueue(Queue):
    def __init__(self, partitions, is_fifo=True):
        """
        Deque-based queue (see collections module). Efficient queue for LIFO and FIFO strategies.
        :param partitions: int count of partitions
        :param type: bool, True for FIFO, False for LIFO
        """
        self.partitions = [i for i in range(0, partitions)]
        self.partitioner = Crc32NamePartitioner(self.partitions)
        self.queues = {
            partition: deque()
            for partition in self.partitions
        }
        self.is_fifo = is_fifo

    def count(self):
        return sum(map(len, six.itervalues(self.queues)))

    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        batch = []

        if self.is_fifo:
            pop_op = self.queues[partition_id].popleft
        else:
            pop_op = self.queues[partition_id].pop

        while max_n_requests > 0 and self.queues[partition_id]:
            batch.append(pop_op())
            max_n_requests -= 1

        return batch

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

                self.queues[partition_id].append(request)
