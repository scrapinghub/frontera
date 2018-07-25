from frontera.core.models import Request, Response
from frontera.worker.db import DBWorker, ScoringConsumer, IncomingConsumer, BatchGenerator
from frontera.settings import Settings
from frontera.core.components import States
import unittest


r1 = Request('http://www.example.com/', meta={b'fingerprint': b'1', b'state': States.DEFAULT, b'jid': 0, b'domain':{b'name':'www.example.com'}})
r2 = Request('http://www.scrapy.org/', meta={b'fingerprint': b'2', b'state': States.DEFAULT, b'jid': 0, b'domain':{b'name':'www.scrapy.org'}})
r3 = Request('https://www.dmoz.org', meta={b'fingerprint': b'3', b'state': States.DEFAULT, b'jid': 0, b'domain':{b'name':'www.dmoz.org'}})


class TestDBWorker(unittest.TestCase):

    def dbw_setup(self, distributed=False):
        settings = Settings()
        settings.MAX_NEXT_REQUESTS = 64
        settings.MESSAGE_BUS = 'tests.mocks.message_bus.FakeMessageBus'
        if distributed:
            settings.BACKEND = 'tests.mocks.components.FakeDistributedBackend'
        else:
            settings.BACKEND = 'tests.mocks.components.FakeBackend'
        return DBWorker(settings, False, False, False, partitions=[0,1,2,3])

    def test_page_crawled(self):
        dbw = self.dbw_setup()
        resp = Response(r1.url, request=r1)
        msg = dbw._encoder.encode_page_crawled(resp)
        incoming_consumer = dbw.slot.components[IncomingConsumer]
        incoming_consumer.spider_log_consumer.put_messages([msg])
        incoming_consumer.run()
        assert set([r.url for r in incoming_consumer.backend.responses]) == set([r1.url])

    def test_links_extracted(self):
        dbw = self.dbw_setup()
        msg = dbw._encoder.encode_links_extracted(r1, [r2, r3])
        incoming_consumer = dbw.slot.components[IncomingConsumer]
        incoming_consumer.spider_log_consumer.put_messages([msg])
        incoming_consumer.run()
        assert set([r.url for r in incoming_consumer.backend.links]) == set([r2.url, r3.url])

    def test_request_error(self):
        dbw = self.dbw_setup()
        msg = dbw._encoder.encode_request_error(r1, 'error')
        incoming_consumer = dbw.slot.components[IncomingConsumer]
        incoming_consumer.spider_log_consumer.put_messages([msg])
        incoming_consumer.run()
        assert incoming_consumer.backend.errors[0][0].url == r1.url
        assert incoming_consumer.backend.errors[0][1] == 'error'

    def test_scoring(self):
        dbw = self.dbw_setup(True)
        batch_gen = dbw.slot.components[BatchGenerator]
        batch_gen.run()
        assert dbw.stats["last_batch_size"] == 0
        msg1 = dbw._encoder.encode_update_score(r1, 0.5, True)
        msg2 = dbw._encoder.encode_update_score(r3, 0.6, True)
        scoring_worker = dbw.slot.components[ScoringConsumer]
        scoring_worker.scoring_log_consumer.put_messages([msg1, msg2])
        scoring_worker.run()
        assert set([r.url for r in dbw.backend.queue.requests]) == set([r1.url, r3.url])
        batch_gen.run()
        assert dbw.stats["last_batch_size"] == 2

    def test_new_batch(self):
        dbw = self.dbw_setup(True)
        batch_gen = dbw.slot.components[BatchGenerator]
        batch_gen.backend.queue.put_requests([r1, r2, r3])
        batch_gen.run()
        assert dbw.stats["last_batch_size"] == 3
        assert set(batch_gen.spider_feed_producer.messages) == \
            set([dbw._encoder.encode_request(r) for r in [r1, r2, r3]])

    def test_offset(self):
        dbw = self.dbw_setup(True)
        incoming_worker = dbw.slot.components[IncomingConsumer]
        batch_gen = dbw.slot.components[BatchGenerator]
        batch_gen.spider_feed = incoming_worker.spider_feed
        batch_gen.spider_feed_producer = incoming_worker.spider_feed_producer
        msg = dbw._encoder.encode_offset(2, 50)
        incoming_worker.spider_log_consumer.put_messages([msg])
        incoming_worker.spider_feed_producer.offset = 100
        incoming_worker.run()
        assert 2 in batch_gen.spider_feed.available_partitions()
        msg1 = dbw._encoder.encode_offset(2, 20)
        msg2 = dbw._encoder.encode_offset(3, 0)
        incoming_worker.spider_log_consumer.put_messages([msg1, msg2])
        incoming_worker.run()
        assert 3 in batch_gen.spider_feed.available_partitions()
        assert 2 not in batch_gen.spider_feed.available_partitions()
        batch_gen.backend.queue.put_requests([r1, r2, r3])
        batch_gen.run()
        assert dbw.stats["last_batch_size"] == 3
        assert 3 in batch_gen.backend.partitions
