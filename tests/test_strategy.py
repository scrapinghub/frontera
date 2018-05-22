# -*- coding: utf-8 -*-
from frontera.worker.strategies import BaseCrawlingStrategy
from frontera.worker.strategy import StatesContext
from frontera.settings import Settings
from frontera.core.manager import FrontierManager

from frontera.contrib.backends.memory import MemoryStates
from frontera.core.components import States
from unittest import TestCase


class TestingCrawlingStrategy(BaseCrawlingStrategy):
    def read_seeds(self, stream):
        pass

    def page_crawled(self, response):
        pass

    def page_error(self, request, error):
        pass

    def links_extracted(self, request, links):
        pass

    def filter_extracted_links(self, request, links):
        pass


class MessageBusStream(object):
    def send(self, request, score=1.0, dont_queue=False):
        pass

    def flush(self):
        pass


class TestCrawlingStrategy(TestCase):
    def strategy(self):
        settings = Settings()
        settings.BACKEND = 'frontera.contrib.backends.sqlalchemy.Distributed'
        manager = FrontierManager.from_settings(settings, db_worker=False, strategy_worker=True)
        stream = MessageBusStream()
        states = MemoryStates(10)
        states_ctx = StatesContext(states)
        return TestingCrawlingStrategy.from_worker(manager, None, stream, states_ctx)

    def test_create_request(self):
        s = self.strategy()
        req = s.create_request("http://test.com/someurl")
        assert req.meta[b'fingerprint'] == b'955ac04f1b1a96de60a5139ad90c80be87822159'

    def test_states_refresh(self):
        s = self.strategy()
        states = s._states_context._states
        url = "http://test.com/someurl"
        req1 = s.create_request(url)
        req1.meta[b'state'] = States.CRAWLED
        states.update_cache(req1)

        req2 = s.create_request(url)
        s.refresh_states([req2])
        assert req2.meta[b'state'] == req1.meta[b'state']
        assert req2.meta[b'state'] == States.CRAWLED