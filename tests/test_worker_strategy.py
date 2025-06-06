import pytest

pytest.importorskip("msgpack")
pytest.importorskip("twisted")

from frontera.core.components import States
from frontera.core.models import Request, Response
from frontera.settings import Settings
from frontera.worker.strategies.bfs import CrawlingStrategy
from frontera.worker.strategy import StrategyWorker

r1 = Request("http://www.example.com/", meta={b"fingerprint": b"1", b"jid": 0})
r2 = Request("http://www.scrapy.org/", meta={b"fingerprint": b"2", b"jid": 0})
r3 = Request("https://www.dmoz.org", meta={b"fingerprint": b"3", b"jid": 0})
r4 = Request("http://www.test.com/some/page", meta={b"fingerprint": b"4", b"jid": 0})


class TestStrategyWorker:
    def sw_setup(self):
        settings = Settings()
        settings.BACKEND = "frontera.contrib.backends.sqlalchemy.Distributed"
        settings.MESSAGE_BUS = "tests.mocks.message_bus.FakeMessageBus"
        settings.SPIDER_LOG_CONSUMER_BATCH_SIZE = 100
        return StrategyWorker(settings, CrawlingStrategy)

    def test_add_seeds(self):
        sw = self.sw_setup()
        msg = sw._encoder.encode_add_seeds([r1, r2, r3, r4])
        sw.consumer.put_messages([msg])
        r2.meta[b"state"] = States.CRAWLED
        sw.states.update_cache([r2])
        sw.work()

        r1.meta[b"state"] = States.QUEUED
        r3.meta[b"state"] = States.QUEUED
        r4.meta[b"state"] = States.QUEUED
        assert set(sw.scoring_log_producer.messages) == {
            sw._encoder.encode_update_score(r, 1.0, True) for r in [r1, r3, r4]
        }

    def test_page_crawled(self):
        sw = self.sw_setup()
        r1.meta[b"jid"] = 1
        resp = Response(r1.url, request=r1)
        msg = sw._encoder.encode_page_crawled(resp)
        sw.consumer.put_messages([msg])
        sw.work()
        # response should be skipped if it's jid doesn't match the strategy worker's
        assert sw.scoring_log_producer.messages == []
        sw.job_id = 1
        sw.consumer.put_messages([msg])
        sw.work()
        r1c = r1.copy()
        sw.states.set_states(r1c)
        assert r1c.meta[b"state"] == States.CRAWLED

    def test_links_extracted(self):
        sw = self.sw_setup()
        sw.job_id = 0
        r1.meta[b"jid"] = 0
        msg = sw._encoder.encode_links_extracted(r1, [r3, r4])
        sw.consumer.put_messages([msg])
        sw.work()
        r3.meta[b"state"] = States.QUEUED
        r4.meta[b"state"] = States.QUEUED
        assert set(sw.scoring_log_producer.messages) == {
            sw._encoder.encode_update_score(r, sw.strategy.get_score(r.url), True)
            for r in [r3, r4]
        }

    def test_request_error(self):
        sw = self.sw_setup()
        msg = sw._encoder.encode_request_error(r4, "error")
        sw.consumer.put_messages([msg])
        sw.work()
        r4.meta[b"state"] = States.ERROR
        assert (
            sw.scoring_log_producer.messages.pop()
            == sw._encoder.encode_update_score(r4, 0.0, False)
        )
