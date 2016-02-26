from __future__ import absolute_import

from cassandra.cluster import Cluster
from cassandra.cqlengine import connection
from cassandra.query import dict_factory
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.management import drop_table

from frontera.core.components import DistributedBackend
from frontera.contrib.backends import CommonBackend
from frontera.contrib.backends.cassandra.components import Metadata, Queue, States
from frontera.utils.misc import load_object


class CassandraBackend(CommonBackend):
    def __init__(self, manager):
        self.manager = manager
        settings = manager.settings
        cluster_ips = settings.get('CASSANDRABACKEND_CLUSTER_IPS')      # Format: ['192.168.0.1', '192.168.0.2']
        cluster_port = settings.get('CASSANDRABACKEND_CLUSTER_PORT')
        drop_all_tables = settings.get('CASSANDRABACKEND_DROP_ALL_TABLES')
        keyspace = settings.get('CASSANDRABACKEND_KEYSPACE')
        keyspace_create = settings.get('CASSANDRABACKEND_CREATE_KEYSPACE_IF_NOT_EXISTS')                # Default: true
        models = settings.get('CASSANDRA_MODELS')

        self.cluster = Cluster(cluster_ips, cluster_port)
        self.models = dict([(name, load_object(klass)) for name, klass in models.items()])

        self.session_cls = self.cluster.connect()
        self.session_cls.row_factory = dict_factory

        if keyspace_create:
            query = """CREATE KEYSPACE IF NOT EXISTS \"%s\"
                        WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}""" % (keyspace, )
            self.session_cls.execute(query)

        self.session_cls.set_keyspace(keyspace)

        connection.set_session(self.session_cls)

        if drop_all_tables:
            for key, value in self.models.iteritems():
                drop_table(key)

        for key, value in self.models.iteritems():
            sync_table(key)

        self._metadata = Metadata(self.session_cls, self.models['MetadataModel'],
                                  settings.get('CASSANDRABACKEND_CACHE_SIZE'))
        self._states = States(self.session_cls, self.models['StateModel'],
                              settings.get('STATE_CACHE_SIZE_LIMIT'))
        self._queue = self._create_queue(settings)

    def frontier_stop(self):
        super(CassandraBackend, self).frontier_stop()
        self.session_cls.shutdown()

    def _create_queue(self, settings):
        return Queue(self.session_cls, self.models['QueueModel'], settings.get('SPIDER_FEED_PARTITIONS'))

    @property
    def queue(self):
        return self._queue

    @property
    def metadata(self):
        return self._metadata

    @property
    def states(self):
        return self._states


class FIFOBackend(CassandraBackend):
    component_name = 'Cassandra FIFO Backend'

    def _create_queue(self, settings):
        return Queue(self.session_cls, self.models['QueueModel'], settings.get('SPIDER_FEED_PARTITIONS'),
                     ordering='created')


class LIFOBackend(CassandraBackend):
    component_name = 'SQLAlchemy LIFO Backend'

    def _create_queue(self, settings):
        return Queue(self.session_cls, self.models['QueueModel'], settings.get('SPIDER_FEED_PARTITIONS'),
                     ordering='created_desc')


class DFSBackend(CassandraBackend):
    component_name = 'Cassandra DFS Backend'

    def _create_queue(self, settings):
        return Queue(self.session_cls, self.models['QueueModel'], settings.get('SPIDER_FEED_PARTITIONS'))

    def _get_score(self, obj):
        return -obj.meta['depth']


class BFSBackend(CassandraBackend):
    component_name = 'Cassandra BFS Backend'

    def _create_queue(self, settings):
        return Queue(self.session_cls, self.models['QueueModel'], settings.get('SPIDER_FEED_PARTITIONS'))

    def _get_score(self, obj):
        return obj.meta['depth']


BASE = CommonBackend
LIFO = LIFOBackend
FIFO = FIFOBackend
DFS = DFSBackend
BFS = BFSBackend


class Distributed(DistributedBackend):
    def __init__(self, manager):
        self.manager = manager
        settings = manager.settings
        cluster_ips = settings.get('CASSANDRABACKEND_CLUSTER_IPS')      # Format: ['192.168.0.1', '192.168.0.2']
        cluster_port = settings.get('CASSANDRABACKEND_CLUSTER_PORT')
        keyspace = settings.get('CASSANDRABACKEND_KEYSPACE')
        keyspace_create = settings.get('CASSANDRABACKEND_CREATE_KEYSPACE_IF_NOT_EXISTS')                # Default: true
        models = settings.get('CASSANDRA_MODELS')

        self.cluster = Cluster(cluster_ips, cluster_port)
        self.models = dict([(name, load_object(klass)) for name, klass in models.items()])

        self.session_cls = self.cluster.connect()
        self.session_cls.row_factory = dict_factory

        if keyspace_create:
            query = """CREATE KEYSPACE IF NOT EXISTS \"%s\"
                        WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}""" % (keyspace, )
            self.session_cls.execute(query)

        self.session_cls.set_keyspace(keyspace)

        connection.set_session(self.session_cls)

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
            model.__table__.drop(bind=b.session_cls)
        model.__table__.create(bind=b.session_cls)

        b._states = States(b.session_cls, model,
                           settings.get('STATE_CACHE_SIZE_LIMIT'))
        return b

    @classmethod
    def db_worker(cls, manager):
        b = cls(manager)
        settings = manager.settings
        drop = settings.get('CASSANDRABACKEND_DROP_ALL_TABLES')

        metadata_m = b.models['MetadataModel']
        queue_m = b.models['QueueModel']
        if drop:
            metadata_m.__table__.drop(bind=b.session_cls)
            queue_m.__table__.drop(bind=b.session_cls)
        metadata_m.__table__.create(bind=b.session_cls)
        queue_m.__table__.create(bind=b.session_cls)

        b._metadata = Metadata(b.session_cls, metadata_m,
                               settings.get('SQLALCHEMYBACKEND_CACHE_SIZE'))
        b._queue = Queue(b.session_cls, queue_m, settings.get('SPIDER_FEED_PARTITIONS'))
        return b

    @property
    def queue(self):
        return self._queue

    @property
    def metadata(self):
        return self._metadata

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

    def add_seeds(self, seeds):
        self.metadata.add_seeds(seeds)

    def get_next_requests(self, max_next_requests, **kwargs):
        partitions = kwargs.pop('partitions', [0])  # TODO: Collect from all known partitions
        batch = []
        for partition_id in partitions:
            batch.extend(self.queue.get_next_requests(max_next_requests, partition_id, **kwargs))
        return batch

    def page_crawled(self, response, links):
        self.metadata.page_crawled(response, links)

    def request_error(self, request, error):
        self.metadata.request_error(request, error)

    def finished(self):
        return NotImplementedError
