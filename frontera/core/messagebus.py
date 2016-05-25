# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod


class BaseStreamConsumer(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_messages(self, timeout=0.1, count=1):
        """
        Returns ``count`` messages from stream, if they are available and operation fits within timeout. If they aren't
        available, tries to get them ``timeout`` seconds time.

        :param timeout: float, time in seconds
        :param count: int, number of messages
        :return: generator with raw messages
        """
        raise NotImplementedError

    @abstractmethod
    def get_offset(self):
        """
        Returns consumer offset.
        :return: int consumer offset
        """
        raise NotImplementedError


class BaseStreamProducer(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def send(self, key, *messages):
        """
        Sending messages to stream.
        :param key: str key used for partitioning, None for non-keyed channels
        :param *messages: encoded message(s)
        """
        raise NotImplementedError

    @abstractmethod
    def flush(self):
        """
        Flushes all internal buffers.
        :return: nothing
        """
        raise NotImplementedError

    @abstractmethod
    def get_offset(self, partition_id):
        """
        Returns producer offset for partition. Raises KeyError, if partition isn't available or doesn't exist.
        :param partition_id: int
        :return: int producer offset
        """
        raise NotImplementedError


class BaseSpiderLogStream(object):
    """
    Spider Log Stream base class. This stream transfers results from spiders to Strategy and DB workers. Any producer
    can write to any partition of this stream. Consumers can be bound to specific partition (SW worker) or not
    bounded (DB worker) to any partition.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def producer(self):
        """
        Creates/returns new producer for spider log. Producing is done by using FingerprintPartitioner.
        :return: BaseStreamProducer instance
        """
        raise NotImplementedError

    @abstractmethod
    def consumer(self, partition_id, type):
        """
        Creates/returns consumer of exact type and bounded to specific partition.
        :param partition_id: int
        :param type: consumer type, can be either "sw" or "db"
        :return: BaseStreamConsumer instance assigned to given partition_id
        """
        raise NotImplementedError


class BaseScoringLogStream(object):
    """
    Scoring log stream base class. This stream is transfering score and scheduling information from Strategy workers to
    DB Workers. This type of stream isn't requiring any partitioning.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def consumer(self):
        """
        :return: BaseStreamConsumer instance
        """
        raise NotImplementedError

    @abstractmethod
    def producer(self):
        """
        :return: BaseStreamProducer instance
        """
        raise NotImplementedError


class BaseSpiderFeedStream(object):
    """
    Spider Feed Stream base class. This stream transfers new batches from DB worker to spiders. Every consumer is
    strictly bounded to specific partition, and producer could write to any partition. This class also has methods
    for reporting of busy/available partitions. DB worker is pushing new batches only to available partitions.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def consumer(self, partition_id):
        """
        Creates/returns spider feed consumer object.
        :param partition_id: int
        :return: BaseStreamConsumer instance assigned to given partition_id
        """
        raise NotImplementedError

    @abstractmethod
    def producer(self):
        """
        Creates/returns spider feed producer object. This producer is meant to use Crc32NamePartitioner
        (separating feed by hosts, so each host will be downloaded by at most one spider).
        :return: BaseStreamProducer instance
        """
        raise NotImplementedError

    @abstractmethod
    def available_partitions(self):
        """
        Returns the iterable of available (ready for processing new batches) partitions.
        :return: iterable of ints
        """
        raise NotImplementedError

    def mark_ready(self, partition_id):
        """
        Marks partition as ready/available for receiving new batches.
        :param partition_id: int
        :return: nothing
        """
        pass

    def mark_busy(self, partition_id):
        """
        Marks partition as busy, so that spider assigned to this partition is busy processing previous batches.
        :param partition_id: int
        :return: nothing
        """
        pass


class BaseMessageBus(object):
    """
    Main message bus class, encapsulating message bus context. Serving as a factory for stream-specific objects.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def scoring_log(self):
        """
        Create or return scoring log stream.
        :return: instance of ScoringLogStream
        """
        raise NotImplementedError

    @abstractmethod
    def spider_log(self):
        """
        Create or return spider log stream.
        :return: instance of SpiderLogStream
        """
        raise NotImplementedError

    @abstractmethod
    def spider_feed(self):
        """
        Create or return spider feed stream.
        :return: instance of SpiderFeedStream
        """
        raise NotImplementedError