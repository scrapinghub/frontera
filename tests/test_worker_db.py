import pytest

pytest.importorskip("msgpack")
pytest.importorskip("twisted")

from frontera.core.components import States
from frontera.core.models import Request, Response
from frontera.settings import Settings
from frontera.worker.db import DBWorker

r1 = Request(
    "http://www.example.com/",
    meta={b"fingerprint": b"1", b"state": States.DEFAULT, b"jid": 0},
)
r2 = Request(
    "http://www.scrapy.org/",
    meta={b"fingerprint": b"2", b"state": States.DEFAULT, b"jid": 0},
)
r3 = Request(
    "https://www.dmoz.org",
    meta={b"fingerprint": b"3", b"state": States.DEFAULT, b"jid": 0},
)


class TestDBWorker:
    def dbw_setup(self, distributed=False):
        settings = Settings()
        settings.MAX_NEXT_REQUESTS = 64
        settings.MESSAGE_BUS = "tests.mocks.message_bus.FakeMessageBus"
        if distributed:
            settings.BACKEND = "tests.mocks.components.FakeDistributedBackend"
        else:
            settings.BACKEND = "tests.mocks.components.FakeBackend"
        return DBWorker(settings, True, True, False)

    def test_add_seeds(self):
        dbw = self.dbw_setup()
        msg = dbw._encoder.encode_add_seeds([r1, r2, r3])
        dbw.spider_log_consumer.put_messages([msg])
        dbw.consume_incoming()
        assert {r.url for r in dbw._backend.seeds} == {r.url for r in [r1, r2, r3]}

    def test_page_crawled(self):
        dbw = self.dbw_setup()
        resp = Response(r1.url, request=r1)
        msg = dbw._encoder.encode_page_crawled(resp)
        dbw.spider_log_consumer.put_messages([msg])
        dbw.consume_incoming()
        assert {r.url for r in dbw._backend.responses} == {r1.url}

    def test_links_extracted(self):
        dbw = self.dbw_setup()
        msg = dbw._encoder.encode_links_extracted(r1, [r2, r3])
        dbw.spider_log_consumer.put_messages([msg])
        dbw.consume_incoming()
        assert {r.url for r in dbw._backend.links} == {r2.url, r3.url}

    def test_request_error(self):
        dbw = self.dbw_setup()
        msg = dbw._encoder.encode_request_error(r1, "error")
        dbw.spider_log_consumer.put_messages([msg])
        dbw.consume_incoming()
        assert dbw._backend.errors[0][0].url == r1.url
        assert dbw._backend.errors[0][1] == "error"

    def test_scoring(self):
        dbw = self.dbw_setup(True)
        msg = dbw._encoder.encode_add_seeds([r1, r2, r3])
        dbw.spider_log_consumer.put_messages([msg])
        dbw.consume_incoming()
        assert dbw.new_batch() == 0
        msg1 = dbw._encoder.encode_update_score(r1, 0.5, True)
        msg2 = dbw._encoder.encode_update_score(r3, 0.6, True)
        dbw.scoring_log_consumer.put_messages([msg1, msg2])
        dbw.consume_scoring()
        assert {r.url for r in dbw._backend.queue.requests} == {r1.url, r3.url}
        assert dbw.new_batch() == 2

    def test_new_batch(self):
        dbw = self.dbw_setup(True)
        dbw._backend.queue.put_requests([r1, r2, r3])
        assert dbw.new_batch() == 3
        assert set(dbw.spider_feed_producer.messages) == {
            dbw._encoder.encode_request(r) for r in [r1, r2, r3]
        }

    def test_offset(self):
        dbw = self.dbw_setup(True)
        msg = dbw._encoder.encode_offset(2, 50)
        dbw.spider_log_consumer.put_messages([msg])
        dbw.spider_feed_producer.offset = 100
        dbw.consume_incoming()
        assert 2 in dbw.spider_feed.available_partitions()
        msg1 = dbw._encoder.encode_offset(2, 20)
        msg2 = dbw._encoder.encode_offset(3, 0)
        dbw.spider_log_consumer.put_messages([msg1, msg2])
        dbw.consume_incoming()
        assert 3 in dbw.spider_feed.available_partitions()
        assert 2 not in dbw.spider_feed.available_partitions()
        dbw._backend.queue.put_requests([r1, r2, r3])
        assert dbw.new_batch() == 3
        assert 3 in dbw._backend.partitions
