from __future__ import absolute_import
import datetime
import logging
import random

from frontera import Backend
from frontera.core.components import Metadata, Queue, States
from frontera.utils.heap import Heap
from frontera.contrib.backends import Crc32NamePartitioner
from frontera.utils.url import parse_domain_from_url_fast


class MemoryMetadata(Metadata):
    def __init__(self):
        self.requests = {}

    def request_error(self, request, error):
        request.meta['error'] = error
        self._get_or_create_request(request)

    def page_crawled(self, response, links):
        self._get_or_create_request(response.request)
        for link in links:
            self._get_or_create_request(link)

    def add_seeds(self, seeds):
        for seed in seeds:
            self._get_or_create_request(seed)

    def _get_or_create_request(self, request):
        fingerprint = request.meta['fingerprint']
        if fingerprint not in self.requests:
            new_request = request.copy()
            self.requests[fingerprint] = new_request
            return new_request, True
        else:
            page = self.requests[fingerprint]
            return page, False


class MemoryQueue(Queue):
    def __init__(self, partitions):
        self.partitions = [i for i in range(0, partitions)]
        self.partitioner = Crc32NamePartitioner(self.partitions)
        self.logger = logging.getLogger("frontera.contrib.backends.memory.MemoryQueue")
        self.heap={}
        for partition in self.partitions:
            self.heap[partition] = Heap(self._compare_pages)

    def count(self):
        return sum(map(lambda h: len(h.heap), self.heap.itervalues()))

    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        return self.heap[partition_id].pop(max_n_requests)

    def schedule(self, batch):
        for fprint, (score, request, schedule) in batch.iteritems():
            if schedule:
                request.meta['_scr'] = score
                _, hostname, _, _, _, _ = parse_domain_from_url_fast(request.url)
                if not hostname:
                    self.logger.error("Can't get hostname for URL %s, fingerprint %s" % (request.url, fprint))
                    continue
                partition_id = self.partitioner.partition(hostname, self.partitions)
                self.heap[partition_id].push(request)

    def _compare_pages(self, first, second):
        return cmp(first.meta['_scr'], second.meta['_scr'])


class MemoryStates(States):

    def __init__(self, cache_size_limit):
        self._cache = dict()
        self._cache_size_limit = cache_size_limit
        self.logger = logging.getLogger("frontera.contrib.backends.memory.MemoryStates")

    def _put(self, obj):
        if obj.meta['state'] is not None:
            self._cache[obj.meta['fingerprint']] = obj.meta['state']

    def _get(self, obj):
        fprint = obj.meta['fingerprint']
        obj.meta['state'] = self._cache[fprint] if fprint in self._cache else None

    def update_cache(self, objs):
        objs = objs if type(objs) in [list, tuple] else [objs]
        map(self._put, objs)

    def set_states(self, objs):
        objs = objs if type(objs) in [list, tuple] else [objs]
        map(self._get, objs)

    def fetch(self, fingerprints):
        pass

    def flush(self, force_clear=False):
        if len(self._cache) > self._cache_size_limit:
            force_clear = True
        if force_clear:
            self.logger.debug("Cache has %d items, clearing" % len(self._cache))
            self._cache.clear()


class MemoryBaseBackend(Backend):
    """
    Base class for in-memory heapq Backend objects.
    """
    component_name = 'Memory Base Backend'

    def __init__(self, manager):
        self.manager = manager
        settings = manager.settings
        self._metadata = MemoryMetadata()
        self._states = MemoryStates(settings.get("STATE_CACHE_SIZE"))
        self._queue = self._create_queue(settings)

    @property
    def metadata(self):
        return self._metadata

    @property
    def states(self):
        return self._states

    @property
    def queue(self):
        return self._queue

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def _create_queue(self, settings):
        return MemoryQueue(1)

    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass

    def add_seeds(self, seeds):
        for seed in seeds: seed.meta['depth'] = 0
        self.metadata.add_seeds(seeds)
        self.states.fetch([seed.meta['fingerprint'] for seed in seeds])
        self.states.set_states(seeds)
        self._schedule(seeds)

    def _schedule(self, requests):
        batch = {}
        for request in requests:
            schedule = True if request.meta['state'] in [States.NOT_CRAWLED, States.ERROR, None] else False
            batch[request.meta['fingerprint']] = (self._get_score(request), request, schedule)
        self.queue.schedule(batch)

    def _get_score(self, obj):
        return 1.0

    def get_next_requests(self, max_next_requests, **kwargs):
        return self.queue.get_next_requests(max_next_requests, 0, **kwargs)

    def page_crawled(self, response, links):
        # TODO: add canonical url solver for response and links
        response.meta['state'] = States.CRAWLED
        self.states.update_cache(response)
        to_fetch = []
        depth = (response.meta['depth'] if 'depth' in response.meta else 0)+1
        for link in links:
            to_fetch.append(link.meta['fingerprint'])
            link.meta['depth'] = depth
            link.meta['created_at'] = datetime.datetime.utcnow()
        self.states.fetch(to_fetch)
        self.states.set_states(links)
        self.metadata.page_crawled(response, links)
        self._schedule(links)

    def request_error(self, request, error):
        # TODO: add canonical url solver
        request.meta['state'] = States.ERROR
        self.metadata.request_error(request, error)
        self.states.update_cache(request)

    def finished(self):
        return self.queue.count() == 0


class MemoryFIFOQueue(MemoryQueue):
    def _compare_pages(self, first, second):
        return cmp(first.meta['created_at'], second.meta['created_at'])


class MemoryLIFOQueue(MemoryQueue):
    def _compare_pages(self, first, second):
        return cmp(second.meta['created_at'], first.meta['created_at'])


class MemoryDFSQueue(MemoryQueue):
    def _compare_pages(self, first, second):
        return cmp((second.meta['depth'], first.meta['created_at']),
                   (first.meta['depth'], second.meta['created_at']))


class MemoryBFSQueue(MemoryQueue):
    def _compare_pages(self, first, second):
        return cmp((first.meta['depth'], first.meta['created_at']),
                   (second.meta['depth'], second.meta['created_at']))


class MemoryRandomQueue(MemoryQueue):
    def _compare_pages(self, first, second):
        return random.choice([-1, 0, 1])


class MemoryFIFOBackend(MemoryBaseBackend):
    def _create_queue(self, settings):
        return MemoryFIFOQueue(1)


class MemoryLIFOBackend(MemoryBaseBackend):
    def _create_queue(self, settings):
        return MemoryLIFOQueue(1)


class MemoryDFSBackend(MemoryBaseBackend):
    def _create_queue(self, settings):
        return MemoryDFSQueue(1)


class MemoryBFSBackend(MemoryBaseBackend):
    def _create_queue(self, settings):
        return MemoryBFSQueue(1)


class MemoryRandomBackend(MemoryBaseBackend):
    def _create_queue(self, settings):
        return MemoryRandomQueue(1)


BASE = MemoryBaseBackend
FIFO = MemoryFIFOBackend
LIFO = MemoryLIFOBackend
DFS = MemoryDFSBackend
BFS = MemoryBFSBackend
RANDOM = MemoryRandomBackend