from __future__ import absolute_import
from frontera.core.manager import LocalFrontierManager
from frontera.settings import Settings
from frontera.core.models import Request, Response
from frontera.core.components import States
from six.moves import range
from unittest import TestCase


r1 = Request('http://www.example.com', meta={b'fingerprint': b'89e6a0649e06d83370cdf2cbfb05f363934a8d0c'})
r2 = Request('https://www.example.com/some/page', meta={b'fingerprint': b'61aec35fac3a032b3be3a5d07eb9e0024bd89de1'})
r3 = Request('http://example1.com', meta={b'fingerprint': b'758293d800fc9672ae2c68bd083359b74ab9b6c2'})

seeds_blob = b"""http://www.example.com
https://www.example.com/some/page
http://example1.com
"""
from io import BytesIO

SEEDS_FILE = BytesIO(seeds_blob)


class TestFrontierManager(TestCase):

    def setup_frontier_manager(self, settings=None):
        settings = settings or Settings()
        settings.BACKEND = 'tests.mocks.components.FakeBackend'
        settings.MIDDLEWARES = ['frontera.contrib.middlewares.domain.DomainMiddleware',
                                'frontera.contrib.middlewares.fingerprint.UrlFingerprintMiddleware',
                                'tests.mocks.components.FakeMiddleware',
                                'tests.mocks.components.FakeMiddlewareModifySeeds',
                                'tests.mocks.components.FakeMiddlewareModifyResponse',
                                'tests.mocks.components.FakeMiddlewareModifyLinks']
        settings.CANONICAL_SOLVER = 'tests.mocks.components.FakeCanonicalSolver'
        settings.STRATEGY = 'tests.mocks.components.CrawlingStrategy'
        return LocalFrontierManager.from_settings(settings)

    def test_start(self):
        fm = self.setup_frontier_manager()
        assert fm._started is True
        assert fm.backend._started is True
        assert [mw._started for mw in fm.middlewares[-4:]] == [True]*4
        assert fm.canonicalsolver._started is True

    def test_stop(self):
        fm = self.setup_frontier_manager()
        fm.stop()
        assert fm._stopped is True
        assert fm.backend._stopped is True
        assert [mw._stopped for mw in fm.middlewares[-4:]] == [True]*4
        assert fm.canonicalsolver._stopped is True

    def test_properties(self):
        fm = self.setup_frontier_manager()
        assert fm.test_mode == fm.settings.get('TEST_MODE')
        assert fm.max_next_requests == fm.settings.get('MAX_NEXT_REQUESTS')
        assert fm.auto_start == fm.settings.get('AUTO_START')
        assert fm.iteration == 0
        assert fm.n_requests == 0
        assert fm.finished is False

    def test_add_seeds(self):
        fm = self.setup_frontier_manager()
        SEEDS_FILE.seek(0)
        fm.add_seeds(SEEDS_FILE)

        fprints_set = set([r.meta[b'fingerprint'] for r in [r1, r2, r3]])

        #seeds reached backend.
        assert set([r.meta[b'fingerprint'] for r in fm.backend.queue.requests]) == fprints_set
        #seeds reached canonicalsolver
        assert set([r.meta[b'fingerprint'] for r in fm.canonicalsolver.requests]) == fprints_set
        #seeds reached the 4 middlewares.
        assert [set([r.meta[b'fingerprint'] for r in mw.requests]) for mw in fm.middlewares[-4:]] == [fprints_set]*4

    def test_page_crawled(self):
        fm = self.setup_frontier_manager()
        response = Response(r1.url, request=r1)
        fm.page_crawled(response)
        assert response.meta[b'state'] == States.CRAWLED
        assert [mw.responses.pop() for mw in fm.middlewares[-4:]] == [response]*4
        assert fm.canonicalsolver.responses.pop() == response
        assert response.meta[b'test_response'] == 'test'

    def test_links_extracted(self):
        fm = self.setup_frontier_manager()
        response = Response(r1.url, request=r1)
        fm.links_extracted(r1, links=[r2, r3])
        assert set([link.meta[b'fingerprint'] for link in fm.backend.queue.requests]) == set([r.meta[b'fingerprint'] for r in [r2, r3]])
        assert set([link for link in fm.canonicalsolver.links]) == set([r2, r3])
        assert [set([link for link in mw.links]) for mw in fm.middlewares[-4:]] == [set([r2, r3])]*4
        assert [link.meta[b'test_links'] for link in [r2, r3]] == ['test']*2
        assert [link.meta[b'test_links_canonical_solver'] for link in [r2, r3]] == ['test']*2

    def test_get_next_requests(self):
        fm = self.setup_frontier_manager()
        fm.backend.put_requests([r1, r2, r3])
        assert set(fm.get_next_requests(3)) == set([r1, r2, r3])
        assert fm.iteration == 1
        assert fm.n_requests == 3

    def test_request_error(self):
        fm = self.setup_frontier_manager()
        fm.request_error(r1, 'error')
        assert r1.meta[b'state'] == States.ERROR
        assert [mw.errors.pop() for mw in fm.middlewares[-4:]] == [(r1, 'error')]*4
        assert fm.canonicalsolver.errors.pop() == (r1, 'error')

    def test_max_requests_reached(self):
        settings = Settings()
        settings.MAX_REQUESTS = 2
        fm = self.setup_frontier_manager(settings)
        fm.backend.put_requests([r1, r2, r3])
        requests = set(fm.get_next_requests(10))
        assert requests == set([r1, r2]) or requests == set([r2, r3]) or requests == set([r1, r3])
        assert fm.get_next_requests(10) == []
        assert fm.finished is True

    def test_blocking_middleware(self):
        settings = Settings()
        settings.BACKEND = 'tests.mocks.components.FakeBackend'
        settings.MIDDLEWARES = ['frontera.contrib.middlewares.domain.DomainMiddleware',
                                'frontera.contrib.middlewares.fingerprint.UrlFingerprintMiddleware',
                                'tests.mocks.components.FakeMiddleware',
                                'tests.mocks.components.FakeMiddlewareModifySeeds',
                                'tests.mocks.components.FakeMiddlewareBlocking',
                                'tests.mocks.components.FakeMiddlewareModifyResponse',
                                'tests.mocks.components.FakeMiddlewareModifyLinks']
        settings.CANONICAL_SOLVER = 'tests.mocks.components.FakeCanonicalSolver'
        settings.STRATEGY = 'tests.mocks.components.CrawlingStrategy'
        fm = LocalFrontierManager.from_settings(settings)
        SEEDS_FILE.seek(0)
        fm.add_seeds(SEEDS_FILE)
        response = Response(r1.url, request=r1)
        fm.page_crawled(response)
        fm.links_extracted(r1, links=[r2])
        fm.request_error(r3, 'error')

        #the seeds, responses, links and errors have not reached the backend.
        assert [len(list) for list in fm.backend.lists] == [0]*4
        #the 3 seeds reach the first three middlewares.
        assert [len(fm.middlewares[i].requests) for i in range(2, 5)] == [3]*3
        #the error, response and link reached the first three middlewares.
        assert [[len(list) for list in fm.middlewares[i].lists[1:]] for i in range(2, 5)] == [[1]*3]*3
        #the values do not reach the bottom 2 middlewares and the canonical solver.
        assert [[len(list) for list in fm.middlewares[i].lists] for i in range(5, 7)] == [[0]*4]*2
        assert [len(list) for list in fm.canonicalsolver.lists] == [0]*4
