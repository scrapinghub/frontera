# -*- coding: utf-8 -*-
from __future__ import absolute_import

from logging import getLogger
from time import sleep

import six
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from frontera.contrib.backends.partitioners import FingerprintPartitioner, Crc32NamePartitioner
from frontera.contrib.messagebus.kafka.offsets_fetcher import OffsetsFetcherAsync
from frontera.core.messagebus import BaseMessageBus, BaseSpiderLogStream, BaseSpiderFeedStream, \
    BaseStreamConsumer, BaseScoringLogStream, BaseStreamProducer, BaseStatsLogStream
from twisted.internet.task import LoopingCall
from traceback import format_tb
from os.path import join as os_path_join


DEFAULT_BATCH_SIZE = 1024 * 1024
DEFAULT_BUFFER_MEMORY = 130 * 1024 * 1024
DEFAULT_MAX_REQUEST_SIZE = 4 * 1024 * 1024

logger = getLogger("messagebus.kafka")


def _prepare_kafka_ssl_kwargs(cert_path):
    """Prepare SSL kwargs for Kafka producer/consumer."""
    return {
        'security_protocol': 'SSL',
        'ssl_cafile': os_path_join(cert_path, 'ca-cert.pem'),
        'ssl_certfile': os_path_join(cert_path, 'client-cert.pem'),
        'ssl_keyfile': os_path_join(cert_path, 'client-key.pem')
    }


class Consumer(BaseStreamConsumer):
    """
    Used in DB and SW worker. SW consumes per partition.
    """
    def __init__(self, location, enable_ssl, cert_path, topic, group, partition_id):
        self._location = location
        self._group = group
        self._topic = topic
        kwargs = _prepare_kafka_ssl_kwargs(cert_path) if enable_ssl else {}
        self._consumer = KafkaConsumer(
            bootstrap_servers=self._location,
            group_id=self._group,
            max_partition_fetch_bytes=10485760,
            consumer_timeout_ms=100,
            client_id="%s-%s" % (self._topic, str(partition_id) if partition_id is not None else "all"),
            request_timeout_ms=120 * 1000,
            heartbeat_interval_ms=10000,
            **kwargs
        )

        if partition_id is not None:
            self._partitions = [TopicPartition(self._topic, partition_id)]
            self._consumer.assign(self._partitions)
        else:
            self._partitions = [TopicPartition(self._topic, pid) for pid in self._consumer.partitions_for_topic(self._topic)]
            self._consumer.subscribe(topics=[self._topic])

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

    def get_offset(self, partition_id):
        for tp in self._partitions:
            if tp.partition == partition_id:
                return self._consumer.position(tp)
        raise KeyError("Can't find partition %d", partition_id)

    def close(self):
        self._consumer.commit()
        self._consumer.close()


class SimpleProducer(BaseStreamProducer):
    def __init__(self, location, enable_ssl, cert_path, topic, compression, **kwargs):
        self._location = location
        self._topic = topic
        self._compression = compression
        self._create(enable_ssl, cert_path, **kwargs)

    def _create(self, enable_ssl, cert_path, **kwargs):
        max_request_size = kwargs.pop('max_request_size', DEFAULT_MAX_REQUEST_SIZE)
        kwargs.update(_prepare_kafka_ssl_kwargs(cert_path) if enable_ssl else {})
        self._producer = KafkaProducer(bootstrap_servers=self._location,
                                       retries=5,
                                       compression_type=self._compression,
                                       max_request_size=max_request_size,
                                       **kwargs)

    def send(self, key, *messages):
        for msg in messages:
            self._producer.send(self._topic, value=msg)

    def flush(self):
        self._producer.flush()

    def close(self):
        self._producer.close()


class KeyedProducer(BaseStreamProducer):
    def __init__(self, location, enable_ssl, cert_path, topic_done, partitioner, compression, **kwargs):
        self._location = location
        self._topic_done = topic_done
        self._partitioner = partitioner
        self._compression = compression
        max_request_size = kwargs.pop('max_request_size', DEFAULT_MAX_REQUEST_SIZE)
        kwargs.update(_prepare_kafka_ssl_kwargs(cert_path) if enable_ssl else {})
        self._producer = KafkaProducer(bootstrap_servers=self._location,
                                       partitioner=partitioner,
                                       retries=5,
                                       compression_type=self._compression,
                                       max_request_size=max_request_size,
                                       **kwargs)

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
        self._db_group = messagebus.spiderlog_dbw_group
        self._sw_group = messagebus.spiderlog_sw_group
        self._topic = messagebus.topic_done
        self._codec = messagebus.codec
        self._partitions = messagebus.spider_log_partitions
        self._enable_ssl = messagebus.enable_ssl
        self._cert_path = messagebus.cert_path

    def producer(self):
        return KeyedProducer(self._location, self._enable_ssl, self._cert_path, self._topic,
                             FingerprintPartitioner(self._partitions), self._codec,
                             batch_size=DEFAULT_BATCH_SIZE,
                             buffer_memory=DEFAULT_BUFFER_MEMORY)

    def consumer(self, partition_id, type):
        """
        Creates spider log consumer with BaseStreamConsumer interface
        :param partition_id: can be None or integer
        :param type: either 'db' or 'sw'
        :return:
        """
        group = self._sw_group if type == b'sw' else self._db_group
        c = Consumer(self._location, self._enable_ssl, self._cert_path, self._topic, group, partition_id)
        assert len(c._consumer.partitions_for_topic(self._topic)) == self._partitions
        return c


class SpiderFeedStream(BaseSpiderFeedStream):
    def __init__(self, messagebus):
        self._location = messagebus.kafka_location
        self._general_group = messagebus.spider_feed_group
        self._topic = messagebus.topic_todo
        self._max_next_requests = messagebus.max_next_requests
        self._hostname_partitioning = messagebus.hostname_partitioning
        self._enable_ssl = messagebus.enable_ssl
        self._cert_path = messagebus.cert_path
        kwargs = {
            'bootstrap_servers': self._location,
            'topic': self._topic,
            'group_id': self._general_group,
        }
        if self._enable_ssl:
            kwargs.update(_prepare_kafka_ssl_kwargs(self._cert_path))
        self._offset_fetcher = OffsetsFetcherAsync(**kwargs)
        self._codec = messagebus.codec
        self._partitions = messagebus.spider_feed_partitions

    def consumer(self, partition_id):
        c = Consumer(self._location, self._enable_ssl, self._cert_path, self._topic, self._general_group, partition_id)
        assert len(c._consumer.partitions_for_topic(self._topic)) == self._partitions
        return c

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
        return KeyedProducer(self._location, self._enable_ssl, self._cert_path, self._topic, partitioner, self._codec,
                             batch_size=DEFAULT_BATCH_SIZE,
                             buffer_memory=DEFAULT_BUFFER_MEMORY)


class ScoringLogStream(BaseScoringLogStream):
    def __init__(self, messagebus):
        self._topic = messagebus.topic_scoring
        self._group = messagebus.scoringlog_dbw_group
        self._location = messagebus.kafka_location
        self._codec = messagebus.codec
        self._cert_path = messagebus.cert_path
        self._enable_ssl = messagebus.enable_ssl

    def consumer(self):
        return Consumer(self._location, self._enable_ssl, self._cert_path, self._topic, self._group, partition_id=None)

    def producer(self):
        return SimpleProducer(self._location, self._enable_ssl, self._cert_path, self._topic, self._codec,
                              batch_size=DEFAULT_BATCH_SIZE,
                              buffer_memory=DEFAULT_BUFFER_MEMORY)


class StatsLogStream(ScoringLogStream, BaseStatsLogStream):
    """Stats log stream implementation for Kafka message bus.

    The interface is the same as for scoring log stream, so it's better
    to reuse it with proper topic and group.
    """
    def __init__(self, messagebus):
        super(StatsLogStream, self).__init__(messagebus)
        self._topic = messagebus.topic_stats
        self._group = messagebus.statslog_reader_group


class MessageBus(BaseMessageBus):
    def __init__(self, settings):
        self.topic_todo = settings.get('SPIDER_FEED_TOPIC')
        self.topic_done = settings.get('SPIDER_LOG_TOPIC')
        self.topic_scoring = settings.get('SCORING_LOG_TOPIC')
        self.topic_stats = settings.get('STATS_LOG_TOPIC')

        self.spiderlog_dbw_group = settings.get('SPIDER_LOG_DBW_GROUP')
        self.spiderlog_sw_group = settings.get('SPIDER_LOG_SW_GROUP')
        self.scoringlog_dbw_group = settings.get('SCORING_LOG_DBW_GROUP')
        self.statslog_reader_group = settings.get('STATS_LOG_READER_GROUP')
        self.spider_feed_group = settings.get('SPIDER_FEED_GROUP')
        self.spider_partition_id = settings.get('SPIDER_PARTITION_ID')
        self.max_next_requests = settings.MAX_NEXT_REQUESTS
        self.hostname_partitioning = settings.get('QUEUE_HOSTNAME_PARTITIONING')
        self.codec = settings.get('KAFKA_CODEC')
        self.kafka_location = settings.get('KAFKA_LOCATION')
        self.enable_ssl = settings.get('KAFKA_ENABLE_SSL')
        self.cert_path = settings.get('KAFKA_CERT_PATH')
        self.spider_log_partitions = settings.get('SPIDER_LOG_PARTITIONS')
        self.spider_feed_partitions = settings.get('SPIDER_FEED_PARTITIONS')

    def spider_log(self):
        return SpiderLogStream(self)

    def spider_feed(self):
        return SpiderFeedStream(self)

    def scoring_log(self):
        return ScoringLogStream(self)

    def stats_log(self):
        return StatsLogStream(self)
