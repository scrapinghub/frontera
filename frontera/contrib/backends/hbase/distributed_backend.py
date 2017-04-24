from __future__ import absolute_import

import logging

from random import choice

from happybase import Connection

from frontera.core.components import DistributedBackend

from .metadata import HBaseMetadata
from .queue import HBaseQueue
from .states import HBaseState


class HBaseBackend(DistributedBackend):
    component_name = 'HBase Backend'

    def __init__(self, manager):
        self.manager = manager
        self.logger = logging.getLogger("hbase.backend")
        settings = manager.settings
        port = settings.get('HBASE_THRIFT_PORT')
        hosts = settings.get('HBASE_THRIFT_HOST')
        namespace = settings.get('HBASE_NAMESPACE')
        self._min_requests = settings.get('BC_MIN_REQUESTS')
        self._min_hosts = settings.get('BC_MIN_HOSTS')
        self._max_requests_per_host = settings.get('BC_MAX_REQUESTS_PER_HOST')

        self.queue_partitions = settings.get('SPIDER_FEED_PARTITIONS')
        host = choice(hosts) if isinstance(hosts, (list, tuple)) else hosts
        kwargs = {
            'host': host,
            'port': int(port),
            'table_prefix': namespace,
            'table_prefix_separator': ':'
        }

        if settings.get('HBASE_USE_FRAMED_COMPACT'):
            kwargs.update({
                'protocol': 'compact',
                'transport': 'framed'
            })

        self.connection = Connection(**kwargs)
        self._metadata = None
        self._queue = None
        self._states = None

    @classmethod
    def strategy_worker(cls, manager):
        o = cls(manager)
        settings = manager.settings
        o._states = HBaseState(
            o.connection,
            settings.get('HBASE_METADATA_TABLE'),
            settings.get('HBASE_STATE_CACHE_SIZE_LIMIT'),
        )

        return o

    @classmethod
    def db_worker(cls, manager):
        o = cls(manager)
        settings = manager.settings
        drop_all_tables = settings.get('HBASE_DROP_ALL_TABLES')
        o._queue = HBaseQueue(
            o.connection,
            o.queue_partitions,
            settings.get('HBASE_QUEUE_TABLE'),
            drop=drop_all_tables,
        )
        o._metadata = HBaseMetadata(
            o.connection,
            settings.get('HBASE_METADATA_TABLE'),
            drop_all_tables,
            settings.get('HBASE_USE_SNAPPY'),
            settings.get('HBASE_BATCH_SIZE'),
            settings.get('STORE_CONTENT'),
        )

        return o

    @property
    def metadata(self):
        return self._metadata

    @property
    def queue(self):
        return self._queue

    @property
    def states(self):
        return self._states

    def frontier_start(self):
        for component in [self.metadata, self.queue, self.states]:
            if component:
                component.frontier_start()

    def frontier_stop(self):
        for component in [self.metadata, self.queue, self.states]:
            if component:
                component.frontier_stop()

        self.connection.close()

    def add_seeds(self, seeds):
        self.metadata.add_seeds(seeds)

    def page_crawled(self, response):
        self.metadata.page_crawled(response)

    def links_extracted(self, request, links):
        self.metadata.links_extracted(request, links)

    def request_error(self, page, error):
        self.metadata.request_error(page, error)

    def finished(self):
        raise NotImplementedError

    def get_next_requests(self, max_next_requests, **kwargs):
        next_pages = []
        partitions = set(kwargs.pop('partitions', []))

        self.logger.debug("Querying queue table.")

        for partition_id in range(0, self.queue_partitions):
            if partition_id not in partitions:
                continue

            results = self.queue.get_next_requests(
                max_next_requests,
                partition_id,
                min_requests=self._min_requests,
                min_hosts=self._min_hosts,
                max_requests_per_host=self._max_requests_per_host,
            )

            next_pages.extend(results)
            self.logger.debug(
                "Got %d requests for partition id %d", len(results),
                partition_id,
            )

        return next_pages
