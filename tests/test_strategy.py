# -*- coding: utf-8 -*-
from unittest import TestCase

from frontera import FrontierManager
from frontera.core.components import States
from frontera.settings import Settings
from frontera.worker.strategies import BaseCrawlingStrategy
from frontera.worker.strategy import StatesContext


class DummyCrawlingStrategy(BaseCrawlingStrategy):
    def add_seeds(self, seeds):
        pass

    def page_crawled(self, response):
        pass

    def page_error(self, request, error):
        pass

    def links_extracted(self, request, links):
        pass


class MessageBusStream(object):
    def send(self, request, score=1.0, dont_queue=False):
        pass

    def flush(self):
        pass


class TestCrawlingStrategy(TestCase):
    def setUp(self):
        settings = Settings()
        settings.BACKEND = "frontera.contrib.backends.memory.MemoryDistributedBackend"
        self.manager = FrontierManager.from_settings(settings, db_worker=False, strategy_worker=True)
        self.stream = MessageBusStream()
        self.states_ctx = StatesContext(self.manager.backend.states)
        self.strategy = DummyCrawlingStrategy(self.manager, self.stream, self.states_ctx)

    def test_create_request(self):
        req = self.strategy.create_request("http://test.com/someurl")
        assert req.meta[b'fingerprint'] == b'955ac04f1b1a96de60a5139ad90c80be87822159'

    def test_states_refresh(self):
        states = self.manager.backend.states
        url = "http://test.com/someurl"
        req1 = self.strategy.create_request(url)
        req1.meta[b'state'] = States.CRAWLED
        states.update_cache(req1)

        req2 = self.strategy.create_request(url)
        self.strategy.refresh_states([req2])
        assert req2.meta[b'state'] == req1.meta[b'state']
        assert req2.meta[b'state'] == States.CRAWLED
