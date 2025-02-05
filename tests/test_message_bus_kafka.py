import pytest

pytest.importorskip("kafka")

import logging
import unittest
from random import randint
from sys import stdout

from kafka import KafkaClient
from kafka.errors import KafkaUnavailableError, NoBrokersAvailable
from w3lib.util import to_bytes

from frontera.contrib.messagebus.kafkabus import MessageBus as KafkaMessageBus
from frontera.settings import Settings
from frontera.utils.fingerprint import sha1


class KafkaConsumerPolling:
    """
    This is needed to adapt for Kafka client zero-result attempts to consume messages from topic. There are reasons
    why this could happen: offset out of range or assignment/subscription problems.
    """

    def __init__(self, consumer):
        self._consumer = consumer
        self._buffer = []
        result = self._consumer.get_messages()
        self._buffer.extend(result)

    def get_messages(self, timeout=0.1, count=1):
        result = []
        tries = 2
        while tries and len(result) < count:
            if self._buffer:
                result.extend(self._buffer[:count])
                self._buffer = self._buffer[count:]
            else:
                result.extend(self._consumer.get_messages(timeout=timeout, count=count))
                tries -= 1
        return result

    def close(self):
        self._consumer.close()


class KafkaMessageBusTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig()
        handler = logging.StreamHandler(stdout)
        logger = logging.getLogger("kafka")
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)

        kafka_location = "127.0.0.1:9092"
        try:
            client = KafkaClient(bootstrap_servers=kafka_location)
        except TypeError:  # old kafka-python
            try:
                client = KafkaClient(kafka_location)
            except KafkaUnavailableError:
                raise self.skipTest("No running kafka service")
        except NoBrokersAvailable:
            raise self.skipTest("No running kafka service")
        client.ensure_topic_exists("frontier-todo")
        client.ensure_topic_exists("frontier-done")
        client.ensure_topic_exists("frontier-score")
        client.close()

        settings = Settings()
        settings.set("KAFKA_LOCATION", kafka_location)
        settings.set("SPIDER_FEED_PARTITIONS", 1)
        settings.set("SPIDER_LOG_PARTITIONS", 1)
        settings.set("QUEUE_HOSTNAME_PARTITIONING", True)
        self.messagebus = KafkaMessageBus(settings)
        spiderlog = self.messagebus.spider_log()

        # sw
        self.sw_sl_c = KafkaConsumerPolling(
            spiderlog.consumer(partition_id=0, type=b"sw")
        )

        scoring_log = self.messagebus.scoring_log()
        self.sw_us_p = scoring_log.producer()

        # db
        self.db_sl_c = KafkaConsumerPolling(
            spiderlog.consumer(partition_id=None, type=b"db")
        )
        self.db_us_c = KafkaConsumerPolling(scoring_log.consumer())

        spider_feed = self.messagebus.spider_feed()
        self.db_sf_p = spider_feed.producer()

        # spider
        self.sp_sl_p = spiderlog.producer()
        self.sp_sf_c = KafkaConsumerPolling(spider_feed.consumer(partition_id=0))

    def tearDown(self):
        self.sw_us_p.close()
        self.db_sf_p.close()
        self.sp_sl_p.close()

        self.sw_sl_c.close()
        self.db_sl_c.close()
        self.db_us_c.close()
        self.sp_sf_c.close()

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
        for _m in self.sp_sf_c.get_messages(timeout=0.1, count=512):
            sf_c += 1
        return sf_c

    def sw_activity(self):
        c = 0
        p = 0
        for m in self.sw_sl_c.get_messages(timeout=0.1, count=512):
            if m.startswith(b"http://helloworld.com/"):
                p += 1
                self.sw_us_p.send(None, b"message" + b"0" + b"," + to_bytes(str(c)))
            c += 1
        assert p > 0
        return c

    def db_activity(self, messages):
        sl_c = 0
        us_c = 0

        for _m in self.db_sl_c.get_messages(timeout=0.1, count=512):
            sl_c += 1
        for _m in self.db_us_c.get_messages(timeout=0.1, count=512):
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

    def test_integration(self):
        self.spider_log_activity(64)
        assert self.sw_activity() == 64
        assert self.db_activity(128) == (64, 32)
        assert self.spider_feed_activity() == 128
