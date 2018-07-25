from __future__ import absolute_import

from collections import Iterable

import logging
import six
from frontera.contrib.backends.partitioners import Crc32NamePartitioner
from frontera.core.components import Metadata, Queue, States, DistributedBackend
from frontera.utils.heap import Heap
from frontera.utils.url import parse_domain_from_url_fast
from six.moves import range


def cmp(a, b):
    return (a > b) - (a < b)


class MemoryMetadata(Metadata):
    def __init__(self):
        self.requests = {}

    def request_error(self, request, error):
        request.meta[b'error'] = error
        self._get_or_create_request(request)

    def page_crawled(self, response):
        self._get_or_create_request(response.request)

    def links_extracted(self, request, links):
        for link in links:
            self._get_or_create_request(link)

    def add_seeds(self, seeds):
        for seed in seeds:
            self._get_or_create_request(seed)

    def _get_or_create_request(self, request):
        fingerprint = request.meta[b'fingerprint']
        if fingerprint not in self.requests:
            new_request = request.copy()
            self.requests[fingerprint] = new_request
            return new_request, True
        else:
            page = self.requests[fingerprint]
            return page, False

    def update_score(self, batch):
        pass


class MemoryQueue(Queue):
    def __init__(self, partitions):
        self.partitions = [i for i in range(0, partitions)]
        self.partitioner = Crc32NamePartitioner(self.partitions)
        self.logger = logging.getLogger("memory.queue")
        self.heap = {}
        for partition in self.partitions:
            self.heap[partition] = Heap(self._compare_pages)

    def count(self):
        return sum([len(h.heap) for h in six.itervalues(self.heap)])

    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        return self.heap[partition_id].pop(max_n_requests)

    def schedule(self, batch):
        for fprint, score, request, schedule in batch:
            if schedule:
                request.meta[b'_scr'] = score
                _, hostname, _, _, _, _ = parse_domain_from_url_fast(request.url)
                if not hostname:
                    self.logger.error("Can't get hostname for URL %s, fingerprint %s", request.url, fprint)
                    partition_id = self.partitions[0]
                else:
                    partition_id = self.partitioner.partition(hostname, self.partitions)
                self.heap[partition_id].push(request)

    def _compare_pages(self, first, second):
        return cmp(first.meta[b'_scr'], second.meta[b'_scr'])


class MemoryStates(States):

    def __init__(self, cache_size_limit):
        self._cache = dict()
        self._cache_size_limit = cache_size_limit
        self.logger = logging.getLogger("memory.states")

    def _put(self, obj):
        self._cache[obj.meta[b'fingerprint']] = obj.meta[b'state']

    def _get(self, obj):
        fprint = obj.meta[b'fingerprint']
        obj.meta[b'state'] = self._cache[fprint] if fprint in self._cache else States.DEFAULT

    def update_cache(self, objs):
        objs = objs if isinstance(objs, Iterable) else [objs]
        [self._put(obj) for obj in objs]

    def set_states(self, objs):
        objs = objs if isinstance(objs, Iterable) else [objs]
        [self._get(obj) for obj in objs]

    def fetch(self, fingerprints):
        pass

    def flush(self):
        if len(self._cache) > self._cache_size_limit:
            self.logger.debug("Cache has %d items, clearing", len(self._cache))
            self._cache.clear()


class MemoryDistributedBackend(DistributedBackend):
    def __init__(self, manager):
        settings = manager.settings
        self._states = MemoryStates(1000)
        self._queue = MemoryQueue(settings.get('SPIDER_FEED_PARTITIONS'))
        self.queue_partitions = settings.get('SPIDER_FEED_PARTITIONS')
        self._domain_metadata = dict()

    def add_seeds(self, seeds):
        pass

    def page_crawled(self, response):
        pass

    def request_error(self, page, error):
        pass

    def finished(self):
        pass

    def links_extracted(self, request, links):
        pass

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

    def get_next_requests(self, max_n_requests, **kwargs):
        next_pages = []
        partitions = set(kwargs.pop('partitions', []))
        for partition_id in range(0, self.queue_partitions):
            if partition_id not in partitions:
                continue
            results = self.queue.get_next_requests(max_n_requests, partition_id)
            next_pages.extend(results)
            self.logger.debug("Got %d requests for partition id %d", len(results), partition_id)
        return next_pages

    @classmethod
    def strategy_worker(cls, manager):
        return cls(manager)

    @classmethod
    def db_worker(cls, manager):
        return cls(manager)

    @classmethod
    def local(cls, manager):
        return cls(manager)

