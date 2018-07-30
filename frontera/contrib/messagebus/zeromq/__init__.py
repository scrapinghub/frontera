# -*- coding: utf-8 -*-
from __future__ import absolute_import
from time import time, sleep
from struct import pack, unpack
from logging import getLogger

import zmq
import six

from frontera.core.messagebus import BaseMessageBus, BaseSpiderLogStream, BaseStreamConsumer, \
    BaseSpiderFeedStream, BaseScoringLogStream, BaseStatsLogStream, BaseStreamProducer
from frontera.contrib.backends.partitioners import FingerprintPartitioner, Crc32NamePartitioner
from frontera.contrib.messagebus.zeromq.socket_config import SocketConfig
from six.moves import range


class Consumer(BaseStreamConsumer):
    def __init__(self, context, location, partition_id, identity, seq_warnings=False, hwm=1000):
        self.subscriber = context.zeromq.socket(zmq.SUB)
        self.subscriber.connect(location)
        self.subscriber.set(zmq.RCVHWM, hwm)

        filter = identity + pack('>B', partition_id) if partition_id is not None else identity
        self.subscriber.setsockopt(zmq.SUBSCRIBE, filter)
        self.counter = 0
        self.count_global = partition_id is None
        self.logger = getLogger("distributed_frontera.messagebus.zeromq.Consumer(%s-%s)" % (identity, partition_id))
        self.seq_warnings = seq_warnings

        self.stats = context.stats
        self.stat_key = "consumer-%s" % identity
        self.stats[self.stat_key] = 0

    def get_messages(self, timeout=0.1, count=1):
        started = time()
        sleep_time = timeout / 10.0
        while count:
            try:
                msg = self.subscriber.recv_multipart(copy=True, flags=zmq.NOBLOCK)
            except zmq.Again:
                if time() - started > timeout:
                    break
                sleep(sleep_time)
            else:
                partition_seqno, global_seqno = unpack(">II", msg[2])
                seqno = global_seqno if self.count_global else partition_seqno
                if not self.counter:
                    self.counter = seqno
                elif self.counter != seqno:
                    if self.seq_warnings:
                        self.logger.warning("Sequence counter mismatch: expected %d, got %d. Check if system "
                                            "isn't missing messages." % (self.counter, seqno))
                    self.counter = None
                yield msg[1]
                count -= 1
                if self.counter:
                    self.counter += 1
                self.stats[self.stat_key] += 1

    def get_offset(self, partition_id):
        return self.counter


class Producer(BaseStreamProducer):
    def __init__(self, context, location, identity):
        self.identity = identity
        self.sender = context.zeromq.socket(zmq.PUB)
        self.sender.connect(location)
        self.counters = {}
        self.global_counter = 0
        self.stats = context.stats
        self.stat_key = "producer-%s" % identity
        self.stats[self.stat_key] = 0

    def send(self, key, *messages):
        # Guarantee that msg is actually a list or tuple (should always be true)
        if not isinstance(messages, (list, tuple)):
            raise TypeError("msg is not a list or tuple!")

        # Raise TypeError if any message is not encoded as bytes
        if any(not isinstance(m, six.binary_type) for m in messages):
            raise TypeError("all produce message payloads must be type bytes")
        partition = self.partitioner.partition(key)
        counter = self.counters.get(partition, 0)
        for msg in messages:
            self.sender.send_multipart([self.identity + pack(">B", partition), msg,
                                        pack(">II", counter, self.global_counter)])
            counter += 1
            self.global_counter += 1
            if counter == 4294967296:
                counter = 0
            if self.global_counter == 4294967296:
                self.global_counter = 0
            self.stats[self.stat_key] += 1
        self.counters[partition] = counter

    def flush(self):
        pass

    def get_offset(self, partition_id):
        return self.counters.get(partition_id, None)


class SpiderLogProducer(Producer):
    def __init__(self, context, location, partitions):
        super(SpiderLogProducer, self).__init__(context, location, b'sl')
        self.partitioner = FingerprintPartitioner(partitions)


class SpiderLogStream(BaseSpiderLogStream):
    def __init__(self, messagebus):
        self.context = messagebus.context
        self.sw_in_location = messagebus.socket_config.sw_in()
        self.db_in_location = messagebus.socket_config.db_in()
        self.out_location = messagebus.socket_config.spiders_out()
        self.partitions = messagebus.spider_log_partitions

    def producer(self):
        return SpiderLogProducer(self.context, self.out_location, self.partitions)

    def consumer(self, partition_id, type):
        location = self.sw_in_location if type == b'sw' else self.db_in_location
        return Consumer(self.context, location, partition_id, b'sl')


class NonPartitionedProducer(Producer):
    def __init__(self, context, location, identity):
        super(NonPartitionedProducer, self).__init__(context, location, identity)

    def send(self, key, *messages):
        # Guarantee that msg is actually a list or tuple (should always be true)
        if not isinstance(messages, (list, tuple)):
            raise TypeError("msg is not a list or tuple!")

        # Raise TypeError if any message is not encoded as bytes
        if any(not isinstance(m, six.binary_type) for m in messages):
            raise TypeError("all produce message payloads must be type bytes")
        counter = self.counters.get(0, 0)
        for msg in messages:
            self.sender.send_multipart([self.identity, msg, pack(">II", counter, counter)])
            counter += 1
            if counter == 4294967296:
                counter = 0
            self.stats[self.stat_key] += 1
        self.counters[0] = counter


class ScoringLogStream(BaseScoringLogStream):
    def __init__(self, messagebus):
        self.context = messagebus.context
        self.in_location = messagebus.socket_config.sw_out()
        self.out_location = messagebus.socket_config.db_in()

    def consumer(self):
        return Consumer(self.context, self.out_location, None, b'us')

    def producer(self):
        return NonPartitionedProducer(self.context, self.in_location, b'us')


class SpiderFeedProducer(Producer):
    def __init__(self, context, location, partitions, hwm, hostname_partitioning):
        super(SpiderFeedProducer, self).__init__(context, location, b'sf')
        self.partitioner = Crc32NamePartitioner(partitions) if hostname_partitioning else \
            FingerprintPartitioner(partitions)
        self.sender.set(zmq.SNDHWM, hwm)


class SpiderFeedStream(BaseSpiderFeedStream):
    def __init__(self, messagebus):
        self.context = messagebus.context
        self.in_location = messagebus.socket_config.db_out()
        self.out_location = messagebus.socket_config.spiders_in()
        self.partitions = messagebus.spider_feed_partitions
        self.ready_partitions = set(self.partitions)
        self.consumer_hwm = messagebus.spider_feed_rcvhwm
        self.producer_hwm = messagebus.spider_feed_sndhwm
        self.hostname_partitioning = messagebus.hostname_partitioning

    def consumer(self, partition_id):
        return Consumer(self.context, self.out_location, partition_id, b'sf', seq_warnings=True, hwm=self.consumer_hwm)

    def producer(self):
        return SpiderFeedProducer(self.context, self.in_location, self.partitions,
                                  self.producer_hwm, self.hostname_partitioning)

    def available_partitions(self):
        return self.ready_partitions

    def mark_ready(self, partition_id):
        self.ready_partitions.add(partition_id)

    def mark_busy(self, partition_id):
        self.ready_partitions.discard(partition_id)


class DevNullProducer(BaseStreamProducer):
    def send(self, key, *messages):
        pass

    def flush(self):
        pass

    def get_offset(self, partition_id):
        pass


class StatsLogStream(BaseStatsLogStream):
    def __init__(self, messagebus):
        self.context = messagebus.context
        self.in_location = messagebus.socket_config.stats_out()

    def consumer(self):
        pass

    def producer(self):
        return DevNullProducer()


class Context(object):

    zeromq = zmq.Context()
    stats = {}


class MessageBus(BaseMessageBus):
    def __init__(self, settings):
        self.context = Context()
        self.socket_config = SocketConfig(settings.get('ZMQ_ADDRESS'),
                                          settings.get('ZMQ_BASE_PORT'))
        self.spider_log_partitions = [i for i in range(settings.get('SPIDER_LOG_PARTITIONS'))]
        self.spider_feed_partitions = [i for i in range(settings.get('SPIDER_FEED_PARTITIONS'))]
        self.spider_feed_sndhwm = int(settings.get('MAX_NEXT_REQUESTS') * len(self.spider_feed_partitions) * 1.2)
        self.spider_feed_rcvhwm = int(settings.get('MAX_NEXT_REQUESTS') * 2.0)
        self.hostname_partitioning = settings.get('QUEUE_HOSTNAME_PARTITIONING')
        if self.socket_config.is_ipv6:
            self.context.zeromq.setsockopt(zmq.IPV6, True)

    def spider_log(self):
        return SpiderLogStream(self)

    def scoring_log(self):
        return ScoringLogStream(self)

    def spider_feed(self):
        return SpiderFeedStream(self)

    def stats_log(self):
        return StatsLogStream(self)
