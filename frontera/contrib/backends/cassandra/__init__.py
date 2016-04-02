from __future__ import absolute_import
from cassandra.cluster import Cluster
from cassandra.cqlengine import connection
from cassandra.query import dict_factory
from cassandra.policies import RetryPolicy, ConstantReconnectionPolicy
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
        cluster_ips = settings.get('CASSANDRABACKEND_CLUSTER_IPS')
        cluster_port = settings.get('CASSANDRABACKEND_CLUSTER_PORT')
        drop_all_tables = settings.get('CASSANDRABACKEND_DROP_ALL_TABLES')
        keyspace = settings.get('CASSANDRABACKEND_KEYSPACE')
        keyspace_create = settings.get('CASSANDRABACKEND_CREATE_KEYSPACE_IF_NOT_EXISTS')
        models = settings.get('CASSANDRABACKEND_MODELS')
        crawl_id = settings.get('CASSANDRABACKEND_CRAWL_ID')

        self.models = dict([(name, load_object(klass)) for name, klass in models.items()])

        self.cluster = Cluster(
            contact_points=cluster_ips,
            port=cluster_port,
            compression=True,
            default_retry_policy=RetryPolicy(),
            reconnection_policy=ConstantReconnectionPolicy(10, 100)
        )

        self.session = self.cluster.connect()
        self.session.row_factory = dict_factory
        self.session.encoder.mapping[dict] = self.session.encoder.cql_encode_map_collection
        self.crawl_id = crawl_id

        if keyspace_create:
            query = """CREATE KEYSPACE IF NOT EXISTS \"%s\"
                        WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}""" % (keyspace, )
            self.session.execute(query)

        self.session.set_keyspace(keyspace)

        connection.set_session(self.session)

        if drop_all_tables:
            for key, value in self.models.iteritems():
                drop_table(value)

        for key, value in self.models.iteritems():
            sync_table(value)

        self._metadata = Metadata(self.session, self.models['MetadataModel'],
                                  settings.get('CASSANDRABACKEND_CACHE_SIZE'), self.crawl_id)
        self._states = States(self.session, self.models['StateModel'],
                              settings.get('STATE_CACHE_SIZE_LIMIT'), self.crawl_id)
        self._queue = self._create_queue(settings)

    def frontier_stop(self):
        self.states.flush()
        self.session.shutdown()

    def _create_queue(self, settings):
        return Queue(self.session, self.models['QueueModel'], settings.get('SPIDER_FEED_PARTITIONS'),
                     self.crawl_id)

    @property
    def queue(self):
        return self._queue

    @property
    def metadata(self):
        return self._metadata

    @property
    def states(self):
        return self._states

BASE = CassandraBackend


class Distributed(DistributedBackend):
    def __init__(self, manager):
        self.manager = manager
        settings = manager.settings
        cluster_ips = settings.get('CASSANDRABACKEND_CLUSTER_IPS')      # Format: ['192.168.0.1', '192.168.0.2']
        cluster_port = settings.get('CASSANDRABACKEND_CLUSTER_PORT')
        keyspace = settings.get('CASSANDRABACKEND_KEYSPACE')
        keyspace_create = settings.get('CASSANDRABACKEND_CREATE_KEYSPACE_IF_NOT_EXISTS')                # Default: true
        models = settings.get('CASSANDRABACKEND_MODELS')

        self.cluster = Cluster(cluster_ips, cluster_port)
        self.models = dict([(name, load_object(klass)) for name, klass in models.items()])

        self.session = self.cluster.connect()
        self.session.row_factory = dict_factory

        if keyspace_create:
            query = """CREATE KEYSPACE IF NOT EXISTS \"%s\"
                        WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}""" % (keyspace, )
            self.session.execute(query)
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
        crawl_id = settings.get('CASSANDRABACKEND_CRAWL_ID')
        model = b.models['StateModel']

        if drop_all_tables:
            drop_table(model)

        sync_table(model)

        b._states = States(b.session, model,
                           settings.get('STATE_CACHE_SIZE_LIMIT'), crawl_id)
        return b

    @classmethod
    def db_worker(cls, manager):
        b = cls(manager)
        settings = manager.settings
        drop = settings.get('CASSANDRABACKEND_DROP_ALL_TABLES')
        crawl_id = settings.get('CASSANDRABACKEND_CRAWL_ID')

        metadata_m = b.models['MetadataModel']
        queue_m = b.models['QueueModel']
        stats_m = b.models['CrawlStatsModel']
        if drop:
            drop_table(metadata_m)
            drop_table(queue_m)
            drop_table(stats_m)

        sync_table(metadata_m)
        sync_table(queue_m)
        sync_table(stats_m)

        b._metadata = Metadata(b.session, metadata_m,
                               settings.get('CASSANDRABACKEND_CACHE_SIZE'), crawl_id)
        b._queue = Queue(b.session, queue_m, settings.get('SPIDER_FEED_PARTITIONS'), crawl_id)
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


