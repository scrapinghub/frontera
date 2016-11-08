from __future__ import absolute_import

import six
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import drop_table

from frontera.contrib.backends import (CommonDistributedStorageBackend,
                                       CommonStorageBackend)
from frontera.contrib.backends.cassandra.components import (Metadata,
                                                            BroadCrawlingQueue,
                                                            Queue, States)
from frontera.utils.misc import load_object


class CassandraBackend(CommonStorageBackend):

    def __init__(self, manager):
        self.manager = manager
        settings = manager.settings
        cluster_hosts = settings.get('CASSANDRABACKEND_CLUSTER_HOSTS')
        cluster_port = settings.get('CASSANDRABACKEND_CLUSTER_PORT')
        drop_all_tables = settings.get('CASSANDRABACKEND_DROP_ALL_TABLES')
        models = settings.get('CASSANDRABACKEND_MODELS')
        keyspace = settings.get('CASSANDRABACKEND_KEYSPACE')

        self.models = dict([(name, load_object(cls)) for name, cls in six.iteritems(models)])
        cluster_kwargs = {
            'port': cluster_port,
            'compression': True,
        }
        if not connection.cluster:
            connection.setup(cluster_hosts, keyspace, **cluster_kwargs)
            connection.session.default_timeout = settings.get('CASSANDRABACKEND_REQUEST_TIMEOUT')

        if drop_all_tables:
            for name, table in six.iteritems(self.models):
                drop_table(table)

        self._metadata = Metadata(self.models['MetadataModel'], settings.get('CASSANDRABACKEND_CACHE_SIZE'))
        self._states = States(self.models['StateModel'], settings.get('STATE_CACHE_SIZE_LIMIT'))
        self._queue = self._create_queue(settings)

    def frontier_stop(self):
        self.states.flush()
        connection.unregister_connection('default')

    def _create_queue(self, settings):
        return Queue(self.models['QueueModel'], settings.get('SPIDER_FEED_PARTITIONS'))


class FIFOBackend(CassandraBackend):
    component_name = 'Cassandra FIFO Backend'

    def _create_queue(self, settings):
        return Queue(self.models['FifoOrLIfoQueueModel'],
                     settings.get('SPIDER_FEED_PARTITIONS'),
                     ordering='created')


class LIFOBackend(CassandraBackend):
    component_name = 'Cassandra LIFO Backend'

    def _create_queue(self, settings):
        return Queue(self.models['FifoOrLIfoQueueModel'],
                     settings.get('SPIDER_FEED_PARTITIONS'),
                     ordering='created_desc')


class DFSBackend(CassandraBackend):
    component_name = 'Cassandra DFS Backend'

    def _create_queue(self, settings):
        return Queue(self.models['QueueModel'], settings.get('SPIDER_FEED_PARTITIONS'))

    def _get_score(self, obj):
        return -obj.meta[b'depth']


class BFSBackend(CassandraBackend):
    component_name = 'Cassandra BFS Backend'

    def _create_queue(self, settings):
        return Queue(self.models['QueueModel'], settings.get('SPIDER_FEED_PARTITIONS'))

    def _get_score(self, obj):
        return obj.meta[b'depth']


BASE = CassandraBackend
LIFO = LIFOBackend
FIFO = FIFOBackend
DFS = DFSBackend
BFS = BFSBackend


class Distributed(CommonDistributedStorageBackend):
    def __init__(self, manager):
        self.manager = manager
        settings = manager.settings
        cluster_hosts = settings.get('CASSANDRABACKEND_CLUSTER_HOSTS')
        cluster_port = settings.get('CASSANDRABACKEND_CLUSTER_PORT')
        drop_all_tables = settings.get('CASSANDRABACKEND_DROP_ALL_TABLES')
        models = settings.get('CASSANDRABACKEND_MODELS')
        keyspace = settings.get('CASSANDRABACKEND_KEYSPACE')

        self.models = dict([(name, load_object(cls)) for name, cls in six.iteritems(models)])
        cluster_kwargs = {
            'port': cluster_port,
            'compression': True,
        }
        if not connection.cluster:
            connection.setup(cluster_hosts, keyspace, **cluster_kwargs)
            connection.session.default_timeout = settings.get('CASSANDRABACKEND_REQUEST_TIMEOUT')

        if drop_all_tables:
            for name, table in six.iteritems(self.models):
                drop_table(table)

        self._metadata = None
        self._queue = None
        self._states = None

    @classmethod
    def strategy_worker(cls, manager):
        b = cls(manager)
        settings = manager.settings
        b._states = States(b.models['StateModel'], settings.get('STATE_CACHE_SIZE_LIMIT'))
        return b

    @classmethod
    def db_worker(cls, manager):
        b = cls(manager)
        settings = manager.settings
        b._metadata = Metadata(b.models['MetadataModel'], settings.get('CASSANDRABACKEND_CACHE_SIZE'))
        b._queue = BroadCrawlingQueue(b.models['QueueModel'], settings.get('SPIDER_FEED_PARTITIONS'))
        return b

    def frontier_stop(self):
        super(Distributed, self).frontier_stop()
        connection.unregister_connection('default')
