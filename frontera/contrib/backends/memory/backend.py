from __future__ import absolute_import

from frontera.contrib.backends import CommonBackend
from frontera.core import OverusedBuffer

from .dequequeue import MemoryDequeQueue
from .metadata import MemoryMetadata
from .queue import (
    MemoryBFSQueue,
    MemoryDFSQueue,
    MemoryQueue,
    MemoryRandomQueue,
)
from .states import MemoryStates


class MemoryBaseBackend(CommonBackend):
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
        self._id = 0

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

    def add_seeds(self, seeds):
        for seed in seeds:
            seed.meta[b'id'] = self._id
            self._id += 1

        super(MemoryBaseBackend, self).add_seeds(seeds)

    def links_extracted(self, request, links):
        for link in links:
            link.meta[b'id'] = self._id
            self._id += 1

        super(MemoryBaseBackend, self).links_extracted(request, links)

    def finished(self):
        return self.queue.count() == 0


class MemoryFIFOBackend(MemoryBaseBackend):
    def _create_queue(self, settings):
        return MemoryDequeQueue(settings.get('SPIDER_FEED_PARTITIONS'))


class MemoryLIFOBackend(MemoryBaseBackend):
    def _create_queue(self, settings):
        return MemoryDequeQueue(settings.get('SPIDER_FEED_PARTITIONS'), is_fifo=False)


class MemoryDFSBackend(MemoryBaseBackend):
    def _create_queue(self, settings):
        return MemoryDFSQueue(settings.get('SPIDER_FEED_PARTITIONS'))


class MemoryBFSBackend(MemoryBaseBackend):
    def _create_queue(self, settings):
        return MemoryBFSQueue(settings.get('SPIDER_FEED_PARTITIONS'))


class MemoryRandomBackend(MemoryBaseBackend):
    def _create_queue(self, settings):
        return MemoryRandomQueue(settings.get('SPIDER_FEED_PARTITIONS'))


class MemoryDFSOverusedBackend(MemoryDFSBackend):
    def __init__(self, manager):
        super(MemoryDFSOverusedBackend, self).__init__(manager)

        self.overused_buffer = OverusedBuffer(
            super(MemoryDFSOverusedBackend, self).get_next_requests,
        )

    def get_next_requests(self, max_next_requests, **kwargs):
        return self.overused_buffer.get_next_requests(max_next_requests, **kwargs)
