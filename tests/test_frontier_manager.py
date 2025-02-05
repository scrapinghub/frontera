from frontera.core.manager import FrontierManager
from frontera.core.models import Request, Response
from frontera.settings import Settings

r1 = Request("http://www.example.com")
r2 = Request("https://www.example.com/some/page")
r3 = Request("http://example1.com")


class TestFrontierManager:
    def setup_frontier_manager(self, settings=None):
        settings = settings or Settings()
        settings.BACKEND = "tests.mocks.components.FakeBackend"
        settings.MIDDLEWARES = [
            "tests.mocks.components.FakeMiddleware",
            "tests.mocks.components.FakeMiddlewareModifySeeds",
            "tests.mocks.components.FakeMiddlewareModifyResponse",
            "tests.mocks.components.FakeMiddlewareModifyLinks",
        ]
        settings.CANONICAL_SOLVER = "tests.mocks.components.FakeCanonicalSolver"
        return FrontierManager.from_settings(settings)

    def test_start(self):
        fm = self.setup_frontier_manager()
        assert fm._started is True
        assert fm.backend._started is True
        assert [mw._started for mw in fm.middlewares] == [True] * 4
        assert fm.canonicalsolver._started is True

    def test_stop(self):
        fm = self.setup_frontier_manager()
        fm.stop()
        assert fm._stopped is True
        assert fm.backend._stopped is True
        assert [mw._stopped for mw in fm.middlewares] == [True] * 4
        assert fm.canonicalsolver._stopped is True

    def test_properties(self):
        fm = self.setup_frontier_manager()
        assert fm.test_mode == fm.settings.get("TEST_MODE")
        assert fm.max_next_requests == fm.settings.get("MAX_NEXT_REQUESTS")
        assert fm.auto_start == fm.settings.get("AUTO_START")
        assert fm.iteration == 0
        assert fm.n_requests == 0
        assert fm.finished is False

    def test_add_seeds(self):
        fm = self.setup_frontier_manager()
        fm.add_seeds([r1, r2, r3])

        # seeds reached backend.
        assert set(fm.backend.seeds) == {r1, r2, r3}
        # seeds reached canonicalsolver
        assert set(fm.canonicalsolver.seeds) == {r1, r2, r3}
        # seeds reached the 4 middlewares.
        assert [set(mw.seeds) for mw in fm.middlewares] == [{r1, r2, r3}] * 4
        # seeds were modified.
        assert [seed.meta[b"test_seeds"] for seed in [r1, r2, r3]] == ["test"] * 3
        assert [seed.meta[b"test_seeds_canonical_solver"] for seed in [r1, r2, r3]] == [
            "test"
        ] * 3

    def test_page_crawled(self):
        fm = self.setup_frontier_manager()
        response = Response(r1.url, request=r1)
        fm.page_crawled(response)
        assert fm.backend.responses.pop() == response
        assert [mw.responses.pop() for mw in fm.middlewares] == [response] * 4
        assert fm.canonicalsolver.responses.pop() == response
        assert response.meta[b"test_response"] == "test"

    def test_links_extracted(self):
        fm = self.setup_frontier_manager()
        Response(r1.url, request=r1)
        fm.links_extracted(r1, links=[r2, r3])
        assert set(fm.backend.links) == {r2, r3}
        assert set(fm.canonicalsolver.links) == {r2, r3}
        assert [set(mw.links) for mw in fm.middlewares] == [{r2, r3}] * 4
        assert [link.meta[b"test_links"] for link in [r2, r3]] == ["test"] * 2
        assert [link.meta[b"test_links_canonical_solver"] for link in [r2, r3]] == [
            "test"
        ] * 2

    def test_get_next_requests(self):
        fm = self.setup_frontier_manager()
        fm.backend.put_requests([r1, r2, r3])
        assert set(fm.get_next_requests(3)) == {r1, r2, r3}
        assert fm.iteration == 1
        assert fm.n_requests == 3

    def test_request_error(self):
        fm = self.setup_frontier_manager()
        fm.request_error(r1, "error")
        assert fm.backend.errors.pop() == (r1, "error")
        assert [mw.errors.pop() for mw in fm.middlewares] == [(r1, "error")] * 4
        assert fm.canonicalsolver.errors.pop() == (r1, "error")

    def test_max_requests_reached(self):
        settings = Settings()
        settings.MAX_REQUESTS = 2
        fm = self.setup_frontier_manager(settings)
        fm.backend.put_requests([r1, r2, r3])
        requests = set(fm.get_next_requests(10))
        assert requests in ({r1, r2}, {r2, r3}, {r1, r3})
        assert fm.get_next_requests(10) == []
        assert fm.finished is True

    def test_blocking_middleware(self):
        settings = Settings()
        settings.BACKEND = "tests.mocks.components.FakeBackend"
        settings.MIDDLEWARES = [
            "tests.mocks.components.FakeMiddleware",
            "tests.mocks.components.FakeMiddlewareModifySeeds",
            "tests.mocks.components.FakeMiddlewareBlocking",
            "tests.mocks.components.FakeMiddlewareModifyResponse",
            "tests.mocks.components.FakeMiddlewareModifyLinks",
        ]
        settings.CANONICAL_SOLVER = "tests.mocks.components.FakeCanonicalSolver"
        fm = FrontierManager.from_settings(settings)
        fm.add_seeds([r1, r2, r3])
        response = Response(r1.url, request=r1)
        fm.page_crawled(response)
        fm.links_extracted(r1, links=[r2])
        fm.request_error(r3, "error")

        # the seeds, responses, links and errors have not reached the backend.
        assert [len(list) for list in fm.backend.lists] == [0] * 4
        # the 3 seeds reach the first three middlewares.
        assert [len(fm.middlewares[i].seeds) for i in range(3)] == [3] * 3
        # the error, response and link reached the first three middlewares.
        assert [
            [len(list) for list in fm.middlewares[i].lists[1:]] for i in range(3)
        ] == [[1] * 3] * 3
        # the values do not reach the bottom 2 middlewares and the canonical solver.
        assert [
            [len(list) for list in fm.middlewares[i].lists] for i in range(3, 5)
        ] == [[0] * 4] * 2
        assert [len(list) for list in fm.canonicalsolver.lists] == [0] * 4
