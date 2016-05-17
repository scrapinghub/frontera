# -*- coding: utf-8 -*-
from __future__ import absolute_import
from frontera.core.messagebus import BaseMessageBus, BaseSpiderLogStream, BaseSpiderFeedStream, \
    BaseStreamConsumer, BaseScoringLogStream, BaseStreamProducer

from kafka import KafkaClient, SimpleConsumer, KeyedProducer as KafkaKeyedProducer, SimpleProducer as KafkaSimpleProducer
from kafka.common import BrokerResponseError, MessageSizeTooLargeError
from kafka.protocol import CODEC_SNAPPY

from frontera.contrib.backends.partitioners import FingerprintPartitioner, Crc32NamePartitioner
from frontera.contrib.messagebus.kafka import OffsetsFetcher
from logging import getLogger
from time import sleep

logger = getLogger("messagebus.kafka")


class Consumer(BaseStreamConsumer):
    """
    Used in DB and SW worker. SW consumes per partition.
    """
    def __init__(self, conn, topic, group, partition_id):
        self._conn = conn
        self._group = group
        self._topic = topic
        self._partition_ids = [partition_id] if partition_id is not None else None

        self._cons = None
        self._connect_consumer()

    def _connect_consumer(self):
        if self._cons is None:
            try:
                self._cons = SimpleConsumer(
                    self._conn,
                    self._group,
                    self._topic,
                    partitions=self._partition_ids,
                    buffer_size=1048576,
                    max_buffer_size=10485760)
            except BrokerResponseError:
                self._cons = None
                logger.warning("Could not connect consumer to Kafka server")
                return False
        return True

    def get_messages(self, timeout=0.1, count=1):
        if not self._connect_consumer():
            yield
        while True:
            try:
                for offmsg in self._cons.get_messages(
                        count,
                        timeout=timeout):
                    try:
                        yield offmsg.message.value
                    except ValueError:
                        logger.warning(
                            "Could not decode {0} message: {1}".format(
                                self._topic,
                                offmsg.message.value))
            except Exception, err:
                logger.warning("Error %s" % err)
            finally:
                break

    def get_offset(self):
        return 0


class SimpleProducer(BaseStreamProducer):
    def __init__(self, connection, topic):
        self._connection = connection
        self._topic = topic
        self._create()

    def _create(self):
        self._producer = KafkaSimpleProducer(self._connection, codec=CODEC_SNAPPY)

    def send(self, key, *messages):
        self._producer.send_messages(self._topic, *messages)

    def flush(self):
        self._producer.stop()
        del self._producer
        self._create()

    def get_offset(self, partition_id):
        # Kafka has it's own offset management
        raise KeyError


class KeyedProducer(BaseStreamProducer):
    def __init__(self, connection, topic_done, partitioner_cls):
        self._prod = None
        self._conn = connection
        self._topic_done = topic_done
        self._partitioner_cls = partitioner_cls

    def _connect_producer(self):
        if self._prod is None:
            try:
                self._prod = KafkaKeyedProducer(self._conn, partitioner=self._partitioner_cls, codec=CODEC_SNAPPY)
            except BrokerResponseError:
                self._prod = None
                logger.warning("Could not connect producer to Kafka server")
                return False
        return True

    def send(self, key, *messages):
        success = False
        max_tries = 5
        if self._connect_producer():
            n_tries = 0
            while not success and n_tries < max_tries:
                try:
                    self._prod.send_messages(self._topic_done, key, *messages)
                    success = True
                except MessageSizeTooLargeError, e:
                    logger.error(str(e))
                    break
                except BrokerResponseError:
                    n_tries += 1
                    logger.warning(
                        "Could not send message. Try {0}/{1}".format(
                            n_tries, max_tries)
                    )
                    sleep(1.0)
        return success

    def flush(self):
        if self._prod is not None:
            self._prod.stop()

    def get_offset(self, partition_id):
        # Kafka has it's own offset management
        raise KeyError


class SpiderLogStream(BaseSpiderLogStream):
    def __init__(self, messagebus):
        self._conn = messagebus.conn
        self._db_group = messagebus.general_group
        self._sw_group = messagebus.sw_group
        self._topic_done = messagebus.topic_done

    def producer(self):
        return KeyedProducer(self._conn, self._topic_done, FingerprintPartitioner)

    def consumer(self, partition_id, type):
        """
        Creates spider log consumer with BaseStreamConsumer interface
        :param partition_id: can be None or integer
        :param type: either 'db' or 'sw'
        :return:
        """
        group = self._sw_group if type == 'sw' else self._db_group
        return Consumer(self._conn, self._topic_done, group, partition_id)


class SpiderFeedStream(BaseSpiderFeedStream):
    def __init__(self, messagebus):
        self._conn = messagebus.conn
        self._general_group = messagebus.general_group
        self._topic = messagebus.topic_todo
        self._max_next_requests = messagebus.max_next_requests
        self._hostname_partitioning = messagebus.hostname_partitioning
        self._offset_fetcher = OffsetsFetcher(self._conn, self._topic, self._general_group)

    def consumer(self, partition_id):
        return Consumer(self._conn, self._topic, self._general_group, partition_id)

    def available_partitions(self):
        partitions = []
        lags = self._offset_fetcher.get()
        for partition, lag in lags.iteritems():
            if lag < self._max_next_requests:
                partitions.append(partition)
        return partitions

    def producer(self):
        partitioner = Crc32NamePartitioner if self._hostname_partitioning else FingerprintPartitioner
        return KeyedProducer(self._conn, self._topic, partitioner)


class ScoringLogStream(BaseScoringLogStream):
    def __init__(self, messagebus):
        self._topic = messagebus.topic_scoring
        self._group = messagebus.general_group
        self._conn = messagebus.conn

    def consumer(self):
        return Consumer(self._conn, self._topic, self._group, partition_id=None)

    def producer(self):
        return SimpleProducer(self._conn, self._topic)


class MessageBus(BaseMessageBus):
    def __init__(self, settings):
        server = settings.get('KAFKA_LOCATION')
        self.topic_todo = settings.get('OUTGOING_TOPIC', "frontier-todo")
        self.topic_done = settings.get('INCOMING_TOPIC', "frontier-done")
        self.topic_scoring = settings.get('SCORING_TOPIC')
        self.general_group = settings.get('FRONTIER_GROUP', "general")
        self.sw_group = settings.get('SCORING_GROUP', "strategy-workers")
        self.spider_partition_id = settings.get('SPIDER_PARTITION_ID')
        self.max_next_requests = settings.MAX_NEXT_REQUESTS
        self.hostname_partitioning = settings.get('QUEUE_HOSTNAME_PARTITIONING')

        self.conn = KafkaClient(server)

    def spider_log(self):
        return SpiderLogStream(self)

    def spider_feed(self):
        return SpiderFeedStream(self)

    def scoring_log(self):
        return ScoringLogStream(self)