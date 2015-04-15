from __future__ import absolute_import
import datetime
import random

from frontera import Backend
from frontera.utils.heap import Heap
from frontera.core import OverusedBuffer


class MemoryBaseBackend(Backend):
    """
    Base class for in-memory heapq Backend objects.
    """
    component_name = 'Memory Base Backend'

    def __init__(self, manager):
        self.manager = manager
        self.requests = {}
        self.heap = Heap(self._compare_pages)

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass

    def add_seeds(self, seeds):
        for seed in seeds:
            request, _ = self._get_or_create_request(seed)
            self.heap.push(request)

    def get_next_requests(self, max_next_requests, **kwargs):
        return self.heap.pop(max_next_requests)

    def page_crawled(self, response, links):
        for link in links:
            request, created = self._get_or_create_request(link)
            if created:
                request.meta['depth'] = response.request.meta.get('depth', 0) + 1
                self.heap.push(request)

    def request_error(self, request, error):
        pass

    def _get_or_create_request(self, request):
        fingerprint = request.meta['fingerprint']
        if fingerprint not in self.requests:
            new_request = self._create_request(request)
            self.requests[fingerprint] = new_request
            self.manager.logger.backend.debug('Creating request %s' % new_request)
            return new_request, True
        else:
            page = self.requests[fingerprint]
            self.manager.logger.backend.debug('Request exists %s' % request)
            return page, False

    def _create_request(self, request):
        new_request = request.copy()
        new_request.meta['created_at'] = datetime.datetime.utcnow()
        new_request.meta['depth'] = 0
        return new_request

    def _compare_pages(self, first, second):
        raise NotImplementedError


class MemoryFIFOBackend(MemoryBaseBackend):
    component_name = 'FIFO Memory Backend'

    def _compare_pages(self, first, second):
        return cmp(first.meta['created_at'], second.meta['created_at'])


class MemoryLIFOBackend(MemoryBaseBackend):
    component_name = 'LIFO Memory Backend'

    def _compare_pages(self, first, second):
        return cmp(second.meta['created_at'], first.meta['created_at'])


class MemoryDFSBackend(MemoryBaseBackend):
    component_name = 'DFS Memory Backend'

    def _compare_pages(self, first, second):
        return cmp((second.meta['depth'], first.meta['created_at']),
                   (first.meta['depth'], second.meta['created_at']))


class MemoryBFSBackend(MemoryBaseBackend):
    component_name = 'BFS Memory Backend'

    def _compare_pages(self, first, second):
        return cmp((first.meta['depth'], first.meta['created_at']),
                   (second.meta['depth'], second.meta['created_at']))


class MemoryRandomBackend(MemoryBaseBackend):
    name = 'RANDOM Memory Backend'

    def _compare_pages(self, first, second):
        return random.choice([-1, 0, 1])


class MemoryDFSOverusedBackend(MemoryDFSBackend):
    component_name = 'DFS Memory Backend taking into account overused slots'

    def __init__(self, manager):
        super(MemoryDFSOverusedBackend, self).__init__(manager)
        self._buffer = OverusedBuffer(super(MemoryDFSOverusedBackend, self).get_next_requests,
                                      manager.logger.manager.debug)

    def get_next_requests(self, max_n_requests, **kwargs):
        return self._buffer.get_next_requests(max_n_requests, **kwargs)


class MemoryRandomOverusedBackend(MemoryRandomBackend):
    component_name = 'Random Memory Backend taking into account overused slots'

    def __init__(self, manager):
        super(MemoryRandomOverusedBackend, self).__init__(manager)
        self._buffer = OverusedBuffer(super(MemoryRandomOverusedBackend, self).get_next_requests,
                                      manager.logger.manager.debug)

    def get_next_requests(self, max_n_requests, **kwargs):
        return self._buffer.get_next_requests(max_n_requests, **kwargs)


BASE = MemoryBaseBackend
FIFO = MemoryFIFOBackend
LIFO = MemoryLIFOBackend
DFS = MemoryDFSBackend
BFS = MemoryBFSBackend
RANDOM = MemoryRandomBackend