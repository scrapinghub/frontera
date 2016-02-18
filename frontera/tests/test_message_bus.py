# -*- coding: utf-8 -*-
from frontera.settings import Settings
from frontera.contrib.messagebus.zeromq import MessageBus
from frontera.utils.fingerprint import sha1
from random import randint
from time import sleep


class MessageBusTester(object):
    def __init__(self, settings=Settings()):
        settings.set('SPIDER_FEED_PARTITIONS', 1)
        settings.set('QUEUE_HOSTNAME_PARTITIONING', True)
        self.messagebus = MessageBus(settings)
        spiderlog = self.messagebus.spider_log()

        # sw
        self.sw_sl_c = spiderlog.consumer(partition_id=0, type='sw')
        scoring_log = self.messagebus.scoring_log()
        self.sw_us_p = scoring_log.producer()

        sleep(0.1)

        # db
        self.db_sl_c = spiderlog.consumer(partition_id=None, type='db')
        self.db_us_c = scoring_log.consumer()

        spider_feed = self.messagebus.spider_feed()
        self.db_sf_p = spider_feed.producer()

        sleep(0.1)

        # spider
        self.sp_sl_p = spiderlog.producer()
        self.sp_sf_c = spider_feed.consumer(0)

        sleep(0.1)

    def spider_log_activity(self, messages):
        for i in range(0, messages):
            if i % 2 == 0:
                self.sp_sl_p.send(sha1(str(randint(1, 1000))), 'http://helloworld.com/way/to/the/sun/' + str(0))
            else:
                self.sp_sl_p.send(sha1(str(randint(1, 1000))), 'http://way.to.the.sun' + str(0))

    def spider_feed_activity(self):
        sf_c = 0
        for m in self.sp_sf_c.get_messages(timeout=1.0, count=512):
            sf_c += 1
        return sf_c

    def sw_activity(self):
        c = 0
        for m in self.sw_sl_c.get_messages(timeout=1.0, count=512):
            if m.startswith('http://helloworld.com/'):
                self.sw_us_p.send(None, 'message' + str(0) + "," + str(c))
            c += 1
        return c

    def db_activity(self, messages):

        sl_c = 0
        us_c = 0

        for m in self.db_sl_c.get_messages(timeout=1.0, count=512):
            sl_c += 1
        for m in self.db_us_c.get_messages(timeout=1.0, count=512):
            us_c += 1
        for i in range(0, messages):
            if i % 2 == 0:
                self.db_sf_p.send("newhost", "http://newhost/new/url/to/crawl")
            else:
                self.db_sf_p.send("someotherhost", "http://newhost223/new/url/to/crawl")

        return (sl_c, us_c)


class IPv6MessageBusTester(MessageBusTester):
    """
    Same as MessageBusTester but with ipv6-localhost
    """
    # TODO This class should be used for IPv6 testing. Use the broker on port
    # 5570 for this test.
    def __init__(self):
        settings = Settings()
        settings.set('ZMQ_ADDRESS', '::1')
        super(IPv6MessageBusTester, self).__init__(settings)


def test_zmq_message_bus():
    """
    Test MessageBus with default settings, IPv6 and Star as ZMQ_ADDRESS
    """
    tester = MessageBusTester()

    tester.spider_log_activity(64)
    assert tester.sw_activity() == 64
    assert tester.db_activity(128) == (64, 32)
    assert tester.spider_feed_activity() == 128
