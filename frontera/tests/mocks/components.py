from frontera.core.components import Backend, Middleware, CanonicalSolver


class FakeMiddleware(Middleware):

    def __init__(self):
        self.seeds = []
        self.responses = []
        self.links = []
        self.errors = []
        self.lists = [self.seeds, self.responses, self.links, self.errors]
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

    def add_seeds(self, seeds):
        for seed in seeds:
            self.seeds.append(seed)
        return seeds

    def page_crawled(self, response, links):
        for link in links:
            self.links.append(link)
        self.responses.append(response)
        return response

    def request_error(self, request, error):
        self.errors.append((request, error))
        return request


class FakeBackend(FakeMiddleware, Backend):

    _finished = False
    requests = []

    def finished(self):
        return self._finished

    def put_requests(self, requests):
        for request in requests:
            self.requests.append(request)

    def get_next_requests(self, max_next_requests, **kwargs):
        lst = []
        for i in range(max_next_requests):
            if self.requests:
                lst.append(self.requests.pop())
        return lst


class FakeMiddlewareBlocking(FakeMiddleware):

    def add_seeds(self, seeds):
        for seed in seeds:
            self.seeds.append(seed)

    def page_crawled(self, response, links):
        for link in links:
            self.links.append(link)
        self.responses.append(response)

    def request_error(self, request, error):
        self.errors.append((request, error))


class FakeMiddlewareModifySeeds(FakeMiddleware):

    def add_seeds(self, seeds):
        for seed in seeds:
            self.seeds.append(seed)
            seed.meta['test_seeds'] = self.test_value
        return seeds


class FakeMiddlewareModifyResponse(FakeMiddleware):

    def page_crawled(self, response, links):
        for link in links:
            self.links.append(link)
        self.responses.append(response)
        response.meta['test_response'] = self.test_value
        return response


class FakeMiddlewareModifyLinks(FakeMiddleware):

    def page_crawled(self, response, links):
        for link in links:
            self.links.append(link)
            link.meta['test_links'] = self.test_value
        self.responses.append(response)
        return response


class FakeCanonicalSolver(CanonicalSolver, FakeMiddleware):

    def add_seeds(self, seeds):
        for seed in seeds:
            self.seeds.append(seed)
            seed.meta['test_seeds_canonical_solver'] = self.test_value
        return seeds

    def page_crawled(self, response, links):
        for link in links:
            self.links.append(link)
            link.meta['test_links_canonical_solver'] = self.test_value
        self.responses.append(response)
        return response
