from frontera.worker.strategy import StrategyWorker
from frontera.settings import Settings
from frontera.core.models import Request, Response
from frontera.core.components import States
from tests.mocks.components import CrawlingStrategy
from unittest import TestCase
from os import remove
from os.path import exists

r1 = Request('http://www.example.com/', meta={b'fingerprint': b'1', b'jid': 0})
r2 = Request('http://www.scrapy.org/', meta={b'fingerprint': b'2', b'jid': 0})
r3 = Request('https://www.dmoz.org', meta={b'fingerprint': b'3', b'jid': 0})
r4 = Request('http://www.test.com/some/page', meta={b'fingerprint': b'4', b'jid': 0})


class FilteredLinksCrawlingStrategy(CrawlingStrategy):
    def filter_extracted_links(self, request, links):
        return []


class TestStrategyWorker(TestCase):
    def setUp(self):
        settings = Settings()
        settings.BACKEND = 'frontera.contrib.backends.sqlalchemy.Distributed'
        settings.MESSAGE_BUS = 'tests.mocks.message_bus.FakeMessageBus'
        settings.STRATEGY = 'tests.mocks.components.CrawlingStrategy'
        settings.SPIDER_LOG_CONSUMER_BATCH_SIZE = 100
        self.sw = StrategyWorker(settings, False)

    def tearDown(self):
        if exists("/tmp/test_urls.txt"):
            remove("/tmp/test_urls.txt")
        pass

    def sw_setup_filtered_links(self):
        settings = Settings()
        settings.BACKEND = 'frontera.contrib.backends.sqlalchemy.Distributed'
        settings.MESSAGE_BUS = 'tests.mocks.message_bus.FakeMessageBus'
        settings.STRATEGY = 'tests.test_worker_strategy.FilteredLinksCrawlingStrategy'
        settings.SPIDER_LOG_CONSUMER_BATCH_SIZE = 100
        return StrategyWorker(settings, False)

    def sw_setup_add_seeds(self):
        settings = Settings()
        settings.BACKEND = 'frontera.contrib.backends.sqlalchemy.Distributed'
        settings.MESSAGE_BUS = 'tests.mocks.message_bus.FakeMessageBus'
        settings.SPIDER_LOG_CONSUMER_BATCH_SIZE = 100
        settings.STRATEGY = 'tests.mocks.components.CrawlingStrategy'
        return StrategyWorker(settings, True)

    def test_add_seeds(self):
        sw = self.sw_setup_add_seeds()
        fh = open("/tmp/test_urls.txt", "wb")
        fh.write(b"http://example1.com/\n")
        fh.write(b"http://www.scrapy.org/\n")
        fh.close()

        sw.run("file:///tmp/test_urls.txt")

        assert sw.add_seeds_mode == True
        produced = [sw._decoder.decode(msg) for msg in sw.update_score._producer.messages]
        assert len(produced) == 2
        assert all(msg[0] == 'update_score' for msg in produced)
        assert produced[0][1].url == "http://example1.com/"
        assert produced[1][1].url == "http://www.scrapy.org/"

    def test_page_crawled(self):
        sw = self.sw
        r1.meta[b'jid'] = 1
        resp = Response(r1.url, request=r1)
        msg = sw._encoder.encode_page_crawled(resp)
        sw.consumer.put_messages([msg])
        sw.work()
        # response should be skipped if it's jid doesn't match the strategy worker's
        assert sw.scoring_log_producer.messages == []
        sw.workflow.job_id = 1
        sw.consumer.put_messages([msg])
        sw.work()
        r1c = r1.copy()
        sw.workflow.states_context.states.set_states(r1c)
        assert r1c.meta[b'state'] == States.CRAWLED

    def test_links_extracted(self):
        sw = self.sw
        sw.job_id = 0
        r1.meta[b'jid'] = 0
        msg = sw._encoder.encode_links_extracted(r1, [r3, r4])
        sw.consumer.put_messages([msg])
        sw.work()
        r3.meta[b'state'] = States.QUEUED
        r4.meta[b'state'] = States.QUEUED

        # decoding messages from scoring log
        fprints = set()
        for msg in sw.scoring_log_producer.messages:
            typ, req, score, is_schedule = sw._decoder.decode(msg)
            fprints.add(req.meta[b'fingerprint'])

        assert fprints == set([r.meta[b'fingerprint'] for r in [r3, r4]])

    def test_filter_links_extracted(self):
        sw = self.sw_setup_filtered_links()
        sw.job_id = 0
        r1.meta[b'jid'] = 0
        msg = sw._encoder.encode_links_extracted(r1, [r3, r4])
        sw.consumer.put_messages([msg])
        sw.work()
        r3.meta[b'state'] = States.QUEUED
        r4.meta[b'state'] = States.QUEUED
        assert set(sw.scoring_log_producer.messages) == set()

    def test_request_error(self):
        sw = self.sw
        msg = sw._encoder.encode_request_error(r4, 'error')
        sw.consumer.put_messages([msg])
        sw.work()
        sw.workflow.states_context.states.set_states(r4)

        assert r4.meta[b'state'] == States.ERROR