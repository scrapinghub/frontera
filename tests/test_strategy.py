import pytest

pytest.importorskip("twisted")
sqlalchemy = pytest.importorskip("sqlalchemy.engine")

from frontera.contrib.backends.memory import MemoryStates
from frontera.core.components import States
from frontera.settings import Settings
from frontera.worker.strategies import BaseCrawlingStrategy
from frontera.worker.strategy import StatesContext
from tests.mocks.frontier_manager import FakeFrontierManager


class DummyCrawlingStrategy(BaseCrawlingStrategy):
    def add_seeds(self, seeds):
        pass

    def page_crawled(self, response):
        pass

    def page_error(self, request, error):
        pass

    def links_extracted(self, request, links):
        pass


class MessageBusStream:
    def send(self, request, score=1.0, dont_queue=False):
        pass

    def flush(self):
        pass


class TestCrawlingStrategy:
    def strategy(self):
        settings = Settings()
        manager = FakeFrontierManager(settings)
        stream = MessageBusStream()
        states = MemoryStates(10)
        states_ctx = StatesContext(states)
        return DummyCrawlingStrategy.from_worker(manager, stream, states_ctx)

    def test_create_request(self):
        s = self.strategy()
        req = s.create_request("http://test.com/someurl")
        assert req.meta[b"fingerprint"] == b"955ac04f1b1a96de60a5139ad90c80be87822159"

    def test_states_refresh(self):
        s = self.strategy()
        states = s._states_context._states
        url = "http://test.com/someurl"
        req1 = s.create_request(url)
        req1.meta[b"state"] = States.CRAWLED
        states.update_cache(req1)

        req2 = s.create_request(url)
        s.refresh_states([req2])
        assert req2.meta[b"state"] == req1.meta[b"state"]
        assert req2.meta[b"state"] == States.CRAWLED
