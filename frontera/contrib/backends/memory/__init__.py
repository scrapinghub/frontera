import logging
import random
from collections import deque
from collections.abc import Iterable

from frontera.contrib.backends import CommonBackend
from frontera.contrib.backends.partitioners import Crc32NamePartitioner
from frontera.core import OverusedBuffer
from frontera.core.components import Metadata, Queue, States
from frontera.utils.heap import Heap
from frontera.utils.url import parse_domain_from_url_fast


def cmp(a, b):
    return (a > b) - (a < b)


class MemoryMetadata(Metadata):
    def __init__(self):
        self.requests = {}

    def request_error(self, request, error):
        request.meta[b"error"] = error
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
        fingerprint = request.meta[b"fingerprint"]
        if fingerprint not in self.requests:
            new_request = request.copy()
            self.requests[fingerprint] = new_request
            return new_request, True
        page = self.requests[fingerprint]
        return page, False

    def update_score(self, batch):
        pass


class MemoryQueue(Queue):
    def __init__(self, partitions):
        self.partitions = list(range(partitions))
        self.partitioner = Crc32NamePartitioner(self.partitions)
        self.logger = logging.getLogger("memory.queue")
        self.heap = {}
        for partition in self.partitions:
            self.heap[partition] = Heap(self._compare_pages)

    def count(self):
        return sum([len(h.heap) for h in self.heap.values()])

    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        return self.heap[partition_id].pop(max_n_requests)

    def schedule(self, batch):
        for fprint, score, request, schedule in batch:
            if schedule:
                request.meta[b"_scr"] = score
                _, hostname, _, _, _, _ = parse_domain_from_url_fast(request.url)
                if not hostname:
                    self.logger.error(
                        "Can't get hostname for URL %s, fingerprint %s",
                        request.url,
                        fprint,
                    )
                    partition_id = self.partitions[0]
                else:
                    partition_id = self.partitioner.partition(hostname, self.partitions)
                self.heap[partition_id].push(request)

    def _compare_pages(self, first, second):
        return cmp(first.meta[b"_scr"], second.meta[b"_scr"])


class MemoryDequeQueue(Queue):
    def __init__(self, partitions, is_fifo=True):
        """
        Deque-based queue (see collections module). Efficient queue for LIFO and FIFO strategies.
        :param partitions: int count of partitions
        :param type: bool, True for FIFO, False for LIFO
        """
        self.partitions = list(range(partitions))
        self.partitioner = Crc32NamePartitioner(self.partitions)
        self.logger = logging.getLogger("memory.dequequeue")
        self.queues = {}
        self.is_fifo = is_fifo
        for partition in self.partitions:
            self.queues[partition] = deque()

    def count(self):
        return sum([len(h) for h in self.queues.values()])

    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        batch = []
        pop_op = (
            self.queues[partition_id].popleft
            if self.is_fifo
            else self.queues[partition_id].pop
        )
        while max_n_requests > 0 and self.queues[partition_id]:
            batch.append(pop_op())
            max_n_requests -= 1
        return batch

    def schedule(self, batch):
        for fprint, score, request, schedule in batch:
            if schedule:
                request.meta[b"_scr"] = score
                _, hostname, _, _, _, _ = parse_domain_from_url_fast(request.url)
                if not hostname:
                    self.logger.error(
                        "Can't get hostname for URL %s, fingerprint %s",
                        request.url,
                        fprint,
                    )
                    partition_id = self.partitions[0]
                else:
                    partition_id = self.partitioner.partition(hostname, self.partitions)
                self.queues[partition_id].append(request)


class MemoryStates(States):
    def __init__(self, cache_size_limit):
        self._cache = {}
        self._cache_size_limit = cache_size_limit
        self.logger = logging.getLogger("memory.states")

    def _put(self, obj):
        self._cache[obj.meta[b"fingerprint"]] = obj.meta[b"state"]

    def _get(self, obj):
        fprint = obj.meta[b"fingerprint"]
        obj.meta[b"state"] = self._cache.get(fprint, States.DEFAULT)

    def update_cache(self, objs):
        objs = objs if isinstance(objs, Iterable) else [objs]
        [self._put(obj) for obj in objs]

    def set_states(self, objs):
        objs = objs if isinstance(objs, Iterable) else [objs]
        [self._get(obj) for obj in objs]

    def fetch(self, fingerprints):
        pass

    def flush(self, force_clear=False):
        if len(self._cache) > self._cache_size_limit:
            force_clear = True
        if force_clear:
            self.logger.debug("Cache has %d items, clearing", len(self._cache))
            self._cache.clear()


class MemoryBaseBackend(CommonBackend):
    """
    Base class for in-memory heapq Backend objects.
    """

    component_name = "Memory Base Backend"

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
            seed.meta[b"id"] = self._id
            self._id += 1
        super().add_seeds(seeds)

    def links_extracted(self, request, links):
        for link in links:
            link.meta[b"id"] = self._id
            self._id += 1
        super().links_extracted(request, links)

    def finished(self):
        return self.queue.count() == 0


class MemoryDFSQueue(MemoryQueue):
    def _compare_pages(self, first, second):
        return cmp(
            (second.meta[b"depth"], first.meta[b"id"]),
            (first.meta[b"depth"], second.meta[b"id"]),
        )


class MemoryBFSQueue(MemoryQueue):
    def _compare_pages(self, first, second):
        return cmp(
            (first.meta[b"depth"], first.meta[b"id"]),
            (second.meta[b"depth"], second.meta[b"id"]),
        )


class MemoryRandomQueue(MemoryQueue):
    def _compare_pages(self, first, second):
        return random.choice([-1, 0, 1])  # noqa: S311


class MemoryFIFOBackend(MemoryBaseBackend):
    def _create_queue(self, settings):
        return MemoryDequeQueue(settings.get("SPIDER_FEED_PARTITIONS"))


class MemoryLIFOBackend(MemoryBaseBackend):
    def _create_queue(self, settings):
        return MemoryDequeQueue(settings.get("SPIDER_FEED_PARTITIONS"), is_fifo=False)


class MemoryDFSBackend(MemoryBaseBackend):
    def _create_queue(self, settings):
        return MemoryDFSQueue(settings.get("SPIDER_FEED_PARTITIONS"))


class MemoryBFSBackend(MemoryBaseBackend):
    def _create_queue(self, settings):
        return MemoryBFSQueue(settings.get("SPIDER_FEED_PARTITIONS"))


class MemoryRandomBackend(MemoryBaseBackend):
    def _create_queue(self, settings):
        return MemoryRandomQueue(settings.get("SPIDER_FEED_PARTITIONS"))


class MemoryDFSOverusedBackend(MemoryDFSBackend):
    def __init__(self, manager):
        super().__init__(manager)
        self.overused_buffer = OverusedBuffer(super().get_next_requests)

    def get_next_requests(self, max_next_requests, **kwargs):
        return self.overused_buffer.get_next_requests(max_next_requests, **kwargs)


BASE = MemoryBaseBackend
FIFO = MemoryFIFOBackend
LIFO = MemoryLIFOBackend
DFS = MemoryDFSBackend
BFS = MemoryBFSBackend
RANDOM = MemoryRandomBackend
