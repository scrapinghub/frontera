from __future__ import absolute_import

import six
from cassandra.cluster import Cluster
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import drop_table, sync_table

from frontera.contrib.backends import (CommonDistributedStorageBackend,
                                       CommonStorageBackend)
from frontera.contrib.backends.cassandra.components import (Metadata, Queue,
                                                            States)
from frontera.utils.misc import load_object


class CassandraBackend(CommonStorageBackend):

    queue_component = Queue

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
            'compression': True
        }
        self.cluster = Cluster(contact_points=cluster_hosts, **cluster_kwargs)
        self.session = self.cluster.connect(keyspace)
        connection.setup(cluster_hosts, keyspace, **cluster_kwargs)
        self.session.default_timeout = connection.session.default_timeout = \
            settings.get('CASSANDRABACKEND_REQUEST_TIMEOUT')

        if drop_all_tables:
            for name, table in six.iteritems(self.models):
                drop_table(table)

        for name, table in six.iteritems(self.models):
            sync_table(table)

        self._metadata = Metadata(self.session,
                                  self.models['MetadataModel'],
                                  settings.get('CASSANDRABACKEND_CACHE_SIZE'))
        self._states = States(self.session,
                              self.models['StateModel'],
                              settings.get('STATE_CACHE_SIZE_LIMIT'))
        self._queue = self._create_queue(settings)

    def frontier_stop(self):
        self.states.flush()
        self.session.shutdown()


BASE = CassandraBackend


class Distributed(CommonDistributedStorageBackend):
    def __init__(self, manager):
        self.manager = manager
        settings = manager.settings
        cluster_hosts = settings.get('CASSANDRABACKEND_CLUSTER_HOSTS')
        cluster_port = settings.get('CASSANDRABACKEND_CLUSTER_PORT')
        keyspace = settings.get('CASSANDRABACKEND_KEYSPACE')
        models = settings.get('CASSANDRABACKEND_MODELS')
        cluster_kwargs = {
            'port': cluster_port,
            'compression': True
        }
        self.cluster = Cluster(cluster_hosts, **cluster_kwargs)
        self.models = dict([(name, load_object(cls)) for name, cls in six.iteritems(models)])

        self.session.set_keyspace(keyspace)
        connection.set_session(self.session)

        self._metadata = None
        self._queue = None
        self._states = None

    @classmethod
    def strategy_worker(cls, manager):
        b = cls(manager)
        settings = manager.settings
        drop_all_tables = settings.get('CASSANDRABACKEND_DROP_ALL_TABLES')
        model = b.models['StateModel']

        if drop_all_tables:
            drop_table(model)

        sync_table(model)

        b._states = States(b.session, model, settings.get('STATE_CACHE_SIZE_LIMIT'))
        return b

    @classmethod
    def db_worker(cls, manager):
        b = cls(manager)
        settings = manager.settings
        drop = settings.get('CASSANDRABACKEND_DROP_ALL_TABLES')
        metadata_m = b.models['MetadataModel']
        queue_m = b.models['QueueModel']

        if drop:
            drop_table(metadata_m)
            drop_table(queue_m)

        sync_table(metadata_m)
        sync_table(queue_m)

        b._metadata = Metadata(b.session, metadata_m)
        b._queue = Queue(b.session, queue_m, settings.get('SPIDER_FEED_PARTITIONS'))
        return b
