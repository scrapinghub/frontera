# -*- coding: utf-8 -*-
from __future__ import absolute_import

from logging import getLogger

from kafka import KafkaClient, SimpleConsumer
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.common import BrokerResponseError

from frontera.contrib.backends.partitioners import FingerprintPartitioner, Crc32NamePartitioner
from frontera.contrib.messagebus.kafka import OffsetsFetcher
from logging import getLogger
from time import sleep
import six
from w3lib.util import to_bytes
from frontera.core.messagebus import BaseMessageBus, BaseSpiderLogStream, BaseSpiderFeedStream, \
    BaseStreamConsumer, BaseScoringLogStream, BaseStreamProducer

logger = getLogger("messagebus.kafka")


class DeprecatedConsumer(BaseStreamConsumer):
    """
    Used in DB and SW worker. SW consumes per partition.
    """
    def __init__(self, location, topic, group, partition_id):
        self._location = location
        self._group = group
        self._topic = topic
        self._partition_ids = [partition_id] if partition_id is not None else None

        self._cons = None
        self._connect_consumer()

    def _connect_consumer(self):
        if self._cons is None:
            try:
                self._client = KafkaClient(self._location)
                self._cons = SimpleConsumer(
                    self._client,
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
            except Exception as err:
                logger.warning("Error %s" % err)
            finally:
                break

    def get_offset(self):
        return 0


class Consumer(BaseStreamConsumer):
    """
    Used in DB and SW worker. SW consumes per partition.
    """
    def __init__(self, location, topic, group, partition_id):
        self._location = location
        self._group = group
        self._topic = topic
        self._consumer = KafkaConsumer(
            bootstrap_servers=self._location,
            group_id=self._group,
            max_partition_fetch_bytes=10485760,
            consumer_timeout_ms=100,
            client_id="%s-%s" % (self._topic, str(partition_id) if partition_id else "all")
        )
        if partition_id:
            self._partition_ids = [TopicPartition(self._topic, partition_id)]
            self._consumer.assign(self._partition_ids)
        else:
            self._partition_ids = [TopicPartition(self._topic, pid) for pid in self._consumer.partitions_for_topic(self._topic)]
            self._consumer.subscribe(topics=[self._topic])

        for tp in self._partition_ids:
            self._consumer.committed(tp)
        self._consumer._update_fetch_positions(self._partition_ids)

    def get_messages(self, timeout=0.1, count=1):
        result = []
        while count > 0:
            try:
                m = next(self._consumer)
                result.append(m.value)
                count -= 1
            except StopIteration:
                break
        return result

    def get_offset(self):
        return 0


class SimpleProducer(BaseStreamProducer):
    def __init__(self, location, topic, compression):
        self._location = location
        self._topic = topic
        self._compression = compression
        self._create()

    def _create(self):
        self._producer = KafkaProducer(bootstrap_servers=self._location, retries=5,
                                       compression_type=self._compression)

    def send(self, key, *messages):
        for msg in messages:
            self._producer.send(self._topic, value=msg)

    def flush(self):
        self._producer.flush()


class KeyedProducer(BaseStreamProducer):
    def __init__(self, location, topic_done, partitioner, compression):
        self._location = location
        self._topic_done = topic_done
        self._partitioner = partitioner
        self._compression = compression
        self._producer = KafkaProducer(bootstrap_servers=self._location, partitioner=partitioner, retries=5,
                                       compression_type=self._compression)

    def send(self, key, *messages):
        for msg in messages:
            self._producer.send(self._topic_done, key=key, value=msg)

    def flush(self):
        self._producer.flush()

    def get_offset(self, partition_id):
        pass


class SpiderLogStream(BaseSpiderLogStream):
    def __init__(self, messagebus):
        self._location = messagebus.kafka_location
        self._db_group = messagebus.general_group
        self._sw_group = messagebus.sw_group
        self._topic_done = messagebus.topic_done
        self._compression_type = messagebus.compression_type
        self._partitions = messagebus.spider_log_partitions
        self._consumer_cls = DeprecatedConsumer if messagebus.use_simple_consumer else Consumer

    def producer(self):
        return KeyedProducer(self._location, self._topic_done, FingerprintPartitioner(self._partitions),
                             self._compression_type)

    def consumer(self, partition_id, type):
        """
        Creates spider log consumer with BaseStreamConsumer interface
        :param partition_id: can be None or integer
        :param type: either 'db' or 'sw'
        :return:
        """
        group = self._sw_group if type == 'sw' else self._db_group
        return self._consumer_cls(self._location, self._topic_done, group, partition_id)


class SpiderFeedStream(BaseSpiderFeedStream):
    def __init__(self, messagebus):
        self._location = messagebus.kafka_location
        self._general_group = messagebus.general_group
        self._topic = messagebus.topic_todo
        self._max_next_requests = messagebus.max_next_requests
        self._hostname_partitioning = messagebus.hostname_partitioning
        self._offset_fetcher = OffsetsFetcher(self._location, self._topic, self._general_group)
        self._compression_type = messagebus.compression_type
        self._partitions = messagebus.spider_feed_partitions
        self._consumer_cls = DeprecatedConsumer if messagebus.use_simple_consumer else Consumer

    def consumer(self, partition_id):
        return self._consumer_cls(self._location, self._topic, self._general_group, partition_id)

    def available_partitions(self):
        partitions = []
        lags = self._offset_fetcher.get()
        for partition, lag in six.iteritems(lags):
            if lag < self._max_next_requests:
                partitions.append(partition)
        return partitions

    def producer(self):
        partitioner = Crc32NamePartitioner(self._partitions) if self._hostname_partitioning \
            else FingerprintPartitioner(self._partitions)
        return KeyedProducer(self._location, self._topic, partitioner, self._compression_type)


class ScoringLogStream(BaseScoringLogStream):
    def __init__(self, messagebus):
        self._topic = messagebus.topic_scoring
        self._group = messagebus.general_group
        self._location = messagebus.kafka_location
        self._compression_type = messagebus.compression_type
        self._consumer_cls = DeprecatedConsumer if messagebus.use_simple_consumer else Consumer

    def consumer(self):
        return self._consumer_cls(self._location, self._topic, self._group, partition_id=None)

    def producer(self):
        return SimpleProducer(self._location, self._topic, self._compression_type)


class MessageBus(BaseMessageBus):
    def __init__(self, settings):
        self.topic_todo = settings.get('OUTGOING_TOPIC')
        self.topic_done = settings.get('INCOMING_TOPIC')
        self.topic_scoring = settings.get('SCORING_TOPIC')
        self.general_group = settings.get('FRONTIER_GROUP')
        self.sw_group = settings.get('SCORING_GROUP')
        self.spider_partition_id = settings.get('SPIDER_PARTITION_ID')
        self.max_next_requests = settings.MAX_NEXT_REQUESTS
        self.hostname_partitioning = settings.get('QUEUE_HOSTNAME_PARTITIONING')
        codec = settings.get('KAFKA_CODEC')
        self.codec = codec if codec else CODEC_NONE
        self.kafka_location = settings.get('KAFKA_LOCATION')
        self.spider_log_partitions = settings.get('SPIDER_LOG_PARTITIONS')
        self.spider_feed_partitions = settings.get('SPIDER_FEED_PARTITIONS')
        self.use_simple_consumer = settings.get('KAFKA_USE_SIMPLE_CONSUMER')

    def spider_log(self):
        return SpiderLogStream(self)

    def spider_feed(self):
        return SpiderFeedStream(self)

    def scoring_log(self):
        return ScoringLogStream(self)
