import pytest

pytest.importorskip("zmq")

from random import randint
from time import sleep
from unittest import SkipTest

from flaky import flaky
from w3lib.util import to_bytes

from frontera.contrib.messagebus.zeromq import MessageBus as ZeroMQMessageBus
from frontera.settings import Settings
from frontera.utils.fingerprint import sha1


class MessageBusTester:
    def __init__(self, cls, settings=None):
        if settings is None:
            settings = Settings()
        settings.set("SPIDER_FEED_PARTITIONS", 1)
        settings.set("SPIDER_LOG_PARTITIONS", 1)
        settings.set("QUEUE_HOSTNAME_PARTITIONING", True)
        self.messagebus = cls(settings)
        spiderlog = self.messagebus.spider_log()

        # sw
        self.sw_sl_c = spiderlog.consumer(partition_id=0, type=b"sw")

        scoring_log = self.messagebus.scoring_log()
        self.sw_us_p = scoring_log.producer()

        sleep(0.1)

        # db
        self.db_sl_c = spiderlog.consumer(partition_id=None, type=b"db")
        self.db_us_c = scoring_log.consumer()

        spider_feed = self.messagebus.spider_feed()
        self.db_sf_p = spider_feed.producer()

        sleep(0.1)

        # spider
        self.sp_sl_p = spiderlog.producer()
        self.sp_sf_c = spider_feed.consumer(0)

        sleep(0.1)

    def spider_log_activity(self, messages):
        for i in range(messages):
            if i % 2 == 0:
                self.sp_sl_p.send(
                    sha1(str(randint(1, 1000))),
                    b"http://helloworld.com/way/to/the/sun/" + b"0",
                )
            else:
                self.sp_sl_p.send(
                    sha1(str(randint(1, 1000))), b"http://way.to.the.sun" + b"0"
                )
        self.sp_sl_p.flush()

    def spider_feed_activity(self):
        sf_c = 0
        for _m in self.sp_sf_c.get_messages(timeout=1.0, count=512):
            sf_c += 1
        return sf_c

    def sw_activity(self):
        c = 0
        p = 0
        for m in self.sw_sl_c.get_messages(timeout=1.0, count=512):
            if m.startswith(b"http://helloworld.com/"):
                p += 1
                self.sw_us_p.send(None, b"message" + b"0" + b"," + to_bytes(str(c)))
            c += 1
        if p == 0:
            raise SkipTest("No running zeromq service")
        assert p > 0
        return c

    def db_activity(self, messages):
        sl_c = 0
        us_c = 0

        for _m in self.db_sl_c.get_messages(timeout=1.0, count=512):
            sl_c += 1
        for _m in self.db_us_c.get_messages(timeout=1.0, count=512):
            us_c += 1
        for i in range(messages):
            if i % 2 == 0:
                self.db_sf_p.send(b"newhost", b"http://newhost/new/url/to/crawl")
            else:
                self.db_sf_p.send(
                    b"someotherhost", b"http://newhost223/new/url/to/crawl"
                )
        self.db_sf_p.flush()
        return (sl_c, us_c)


class IPv6MessageBusTester(MessageBusTester):
    """
    Same as MessageBusTester but with ipv6-localhost
    """

    # TODO This class should be used for IPv6 testing. Use the broker on port
    # 5570 for this test.
    def __init__(self):
        settings = Settings()
        settings.set("ZMQ_ADDRESS", "::1")
        super().__init__(settings)


@flaky
def test_zmq_message_bus():
    """
    Test MessageBus with default settings, IPv6 and Star as ZMQ_ADDRESS
    """
    tester = MessageBusTester(ZeroMQMessageBus)
    tester.spider_log_activity(64)
    assert tester.sw_activity() == 64
    assert tester.db_activity(128) == (64, 32)
    assert tester.spider_feed_activity() == 128
