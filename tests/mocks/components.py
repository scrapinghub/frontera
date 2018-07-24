from __future__ import absolute_import
from frontera.core.components import Backend, Middleware, CanonicalSolver, \
    DistributedBackend, Queue
from frontera.contrib.backends.memory import MemoryStates
from six.moves import range
from frontera.core.models import Request
from frontera.strategy import BaseCrawlingStrategy
from frontera.core.components import States


class FakeMiddleware(Middleware):

    def __init__(self):
        self.requests = []
        self.responses = []
        self.links = []
        self.errors = []
        self.lists = [self.requests, self.responses, self.links, self.errors]
        self._started = False
        self._stopped = False
        self.test_value = 'test'

    @classmethod
    def from_manager(cls, manager):
        return cls()

    def frontier_start(self):
        self._started = True

    def frontier_stop(self):
        self._stopped = True

    def create_request(self, request):
        self.requests.append(request)
        return request

    def page_crawled(self, response):
        self.responses.append(response)
        return response

    def links_extracted(self, request, links):
        for link in links:
            self.links.append(link)
        return request

    def request_error(self, request, error):
        self.errors.append((request, error))
        return request


class FakeQueue(Queue):

    def __init__(self):
        self.requests = []

    def put_requests(self, requests):
        for request in requests:
            self.requests.append(request)

    def get_next_requests(self, max_next_requests, **kwargs):
        lst = []
        for i in range(max_next_requests):
            if self.requests:
                lst.append(self.requests.pop())
        return lst

    def count(self):
        return len(self.requests)

    def schedule(self, batch):
        for fingerprint, score, request, is_schedule in batch:
            if is_schedule:
                self.requests.append(request)


class FakeBackend(FakeMiddleware, Backend):

    def __init__(self):
        self._finished = False
        self._queue = FakeQueue()
        self._states = MemoryStates(10000)
        super(FakeBackend, self).__init__()

    @property
    def queue(self):
        return self._queue

    @property
    def states(self):
        return self._states

    def finished(self):
        return self._finished

    def put_requests(self, requests):
        self.queue.put_requests(requests)

    def get_next_requests(self, max_next_requests, **kwargs):
        return self.queue.get_next_requests(max_next_requests, **kwargs)


class FakeDistributedBackend(FakeBackend, DistributedBackend):

    def __init__(self):
        FakeBackend.__init__(self)
        self._queue = FakeQueue()
        self.partitions = set()

    @classmethod
    def db_worker(cls, manager):
        return cls()

    @classmethod
    def strategy_worker(cls, manager):
        return cls()

    @property
    def queue(self):
        return self._queue

    def get_next_requests(self, max_next_request, partitions, **kwargs):
        for partition in partitions:
            self.partitions.add(partition)
        return self._queue.get_next_requests(max_next_request)


class FakeMiddlewareBlocking(FakeMiddleware):

    def add_seeds(self, seeds):
        for seed in seeds:
            self.seeds.append(seed)

    def page_crawled(self, response):
        self.responses.append(response)

    def links_extracted(self, request, links):
        for link in links:
            self.links.append(link)

    def request_error(self, request, error):
        self.errors.append((request, error))

    def create_request(self, request):
        self.requests.append(request)


class FakeMiddlewareModifySeeds(FakeMiddleware):

    def add_seeds(self, seeds):
        for seed in seeds:
            self.seeds.append(seed)
            seed.meta[b'test_seeds'] = self.test_value
        return seeds


class FakeMiddlewareModifyResponse(FakeMiddleware):

    def page_crawled(self, response):
        self.responses.append(response)
        response.meta[b'test_response'] = self.test_value
        return response

    def links_extracted(self, request, links):
        for link in links:
            self.links.append(link)
        return request


class FakeMiddlewareModifyLinks(FakeMiddleware):

    def page_crawled(self, response):
        self.responses.append(response)
        return response

    def links_extracted(self, request, links):
        for link in links:
            self.links.append(link)
            link.meta[b'test_links'] = self.test_value
        return request

class FakeCanonicalSolver(CanonicalSolver, FakeMiddleware):

    def add_seeds(self, seeds):
        for seed in seeds:
            self.seeds.append(seed)
            seed.meta[b'test_seeds_canonical_solver'] = self.test_value
        return seeds

    def page_crawled(self, response):
        self.responses.append(response)
        return response

    def links_extracted(self, request, links):
        for link in links:
            self.links.append(link)
            link.meta[b'test_links_canonical_solver'] = self.test_value
        return request


class CrawlingStrategy(BaseCrawlingStrategy):
    def read_seeds(self, fh):
        for url in fh:
            url = url.strip()
            req = self.create_request(url)
            self.refresh_states(req)
            if req.meta[b'state'] == States.NOT_CRAWLED:
                req.meta[b'state'] = States.QUEUED
                self.schedule(req)

    def page_crawled(self, response):
        response.meta[b'state'] = States.CRAWLED

    def filter_extracted_links(self, request, links):
        return links

    def links_extracted(self, request, links):
        for link in links:
            if link.meta[b'state'] == States.NOT_CRAWLED:
                link.meta[b'state'] = States.QUEUED
                self.schedule(link, 0.5)

    def request_error(self, request, error):
        request.meta[b'state'] = States.ERROR