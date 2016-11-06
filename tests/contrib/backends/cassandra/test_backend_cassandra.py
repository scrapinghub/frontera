import unittest
import uuid
from datetime import datetime
from time import time

import six
from cassandra.cluster import Cluster
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import (create_keyspace_simple,
                                            drop_keyspace, drop_table,
                                            sync_table)

from frontera.contrib.backends.cassandra import CassandraBackend
from frontera.contrib.backends.cassandra.models import (FifoOrLIfoQueueModel,
                                                        MetadataModel,
                                                        QueueModel, StateModel)
from frontera.core.components import States
from frontera.core.models import Request, Response
from frontera.settings import Settings
from tests import backends


r1 = Request('https://www.example.com', meta={b'fingerprint': b'10',
             b'domain': {b'name': b'www.example.com', b'fingerprint': b'81'}})
r2 = Request('http://example.com/some/page/', meta={b'fingerprint': b'11',
             b'domain': {b'name': b'example.com', b'fingerprint': b'82'}})
r3 = Request('http://www.scrapy.org', meta={b'fingerprint': b'12',
             b'domain': {b'name': b'www.scrapy.org', b'fingerprint': b'83'}})
r4 = r3.copy()


class BaseCassandraTest(object):

    def setUp(self):
        settings = Settings()
        hosts = ['127.0.0.1']
        port = 9042
        self.manager = type('manager', (object,), {})
        self.manager.settings = settings
        self.keyspace = settings.CASSANDRABACKEND_KEYSPACE
        timeout = settings.CASSANDRABACKEND_REQUEST_TIMEOUT
        cluster = Cluster(hosts, port)
        self.session = cluster.connect()
        if not connection.cluster:
            connection.setup(hosts, self.keyspace, port=port)
            connection.session.default_timeout = timeout
        create_keyspace_simple(self.keyspace, 1)
        self.session.set_keyspace(self.keyspace)
        self.session.default_timeout = timeout
        connection.session.set_keyspace(self.keyspace)

    def tearDown(self):
        drop_keyspace(self.keyspace)
        self.session.shutdown()


class TestCassandraBackendModels(BaseCassandraTest, unittest.TestCase):

    def test_pickled_fields(self):
        sync_table(MetadataModel)
        m = MetadataModel(fingerprint='fingerprint',
                          url='http://example.com',
                          depth=0,
                          created_at=datetime.now())
        meta = {b'fingerprint': b'10',
                b'scrapy_meta': {'non_binary': 'www.example.com',
                                 'number': 81,
                                 'list': ['str', b'bytes', u'unicode']}
                }
        m.meta = meta
        m.save()
        stored_meta = m.get(fingerprint='fingerprint').meta
        self.assertDictEqual(meta, stored_meta)

    def test_metadata_model(self):
        fields = {
            'fingerprint': 'fingerprint',
            'url': 'http://example.com',
            'depth': 0,
            'created_at': datetime.now(),
            'fetched_at': datetime.now(),
            'status_code': 400,
            'score': 0.9,
            'error': 'Bad Request',
            'meta': {'meta': 'meta'},
            'headers': {'headers': 'headers'},
            'cookies': {'cookies': 'cookies'},
            'method': 'GET',
        }
        self.assert_db_values(MetadataModel, {'fingerprint': fields['fingerprint']}, fields)

    def test_state_model(self):
        fields = {
            'fingerprint': 'fingerprint',
            'state': 1
        }
        self.assert_db_values(StateModel, {'fingerprint': fields['fingerprint']}, fields)

    def test_queue_model(self):
        fields = {
            'id': uuid.uuid4(),
            'partition_id': 0,
            'score': 0.8,
            'url': 'http://example.com',
            'fingerprint': 'fingerprint',
            'host_crc32': 1234,
            'meta': {'meta': 'meta'},
            'headers': {'headers': 'headers'},
            'cookies': {'cookies': 'cookies'},
            'method': 'GET',
            'created_at': int(time()*1E+6),
            'depth': 0,
        }
        for model in [FifoOrLIfoQueueModel, QueueModel]:
            self.assert_db_values(model, {'id': fields['id']}, fields)
            drop_table(model)

    def assert_db_values(self, model, _filter, fields):
        sync_table(model)
        m = model(**fields)
        m.save()
        stored_obj = m.objects.allow_filtering().get(**_filter)
        for field, original_value in six.iteritems(fields):
            stored_value = getattr(stored_obj, field)
            if isinstance(original_value, dict):
                self.assertDictEqual(stored_value, original_value)
            elif isinstance(original_value, datetime):
                self.assertEqual(stored_value.ctime(), original_value.ctime())
            elif isinstance(original_value, float):
                self.assertAlmostEquals(stored_value, original_value)
            else:
                self.assertEqual(stored_value, original_value)


class TestCassandraBackend(BaseCassandraTest, unittest.TestCase):

    def _get_tables(self):
        query = self.session.prepare('SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?')
        result = self.session.execute(query, (self.session.keyspace,))
        return [row.table_name for row in result.current_rows]

    def test_tables_created(self):
        tables_before = self._get_tables()
        self.assertEqual(tables_before, [])
        CassandraBackend(self.manager)
        tables_after = self._get_tables()
        self.assertEqual(set(tables_after), set(['metadata', 'states', 'queue']))

    def test_tables_droped_and_created(self):
        def _get_state_data():
            return StateModel.all()

        models = [MetadataModel, StateModel, QueueModel]
        for model in models:
            sync_table(model)
        tables_before = self._get_tables()
        self.assertEqual(set(tables_before), set(['metadata', 'states', 'queue']))
        StateModel.create(fingerprint='fingerprint', state=200)
        rows_before = _get_state_data()
        self.assertEqual(rows_before.count(), 1)
        self.manager.settings.CASSANDRABACKEND_DROP_ALL_TABLES = True
        CassandraBackend(self.manager)
        self.assertEqual(set(tables_before), set(['metadata', 'states', 'queue']))
        rows_after = _get_state_data()
        self.assertEqual(rows_after.count(), 0)

    def test_metadata(self):
        b = CassandraBackend(self.manager)
        metadata = b.metadata
        metadata.add_seeds([r1, r2, r3])
        meta_qs = MetadataModel.objects.all()
        self.assertEqual(set([r1.url, r2.url, r3.url]), set([m.url for m in meta_qs]))
        resp = Response('https://www.example.com', request=r1)
        metadata.page_crawled(resp)
        stored_response = meta_qs.get(fingerprint='10')
        self.assertEqual(stored_response.status_code, 200)
        metadata.request_error(r3, 'error')
        stored_error = meta_qs.get(fingerprint='12')
        self.assertEqual(stored_error.error, 'error')
        batch = {r2.meta[b'fingerprint']: [0.8, r2.url, False]}
        metadata.update_score(batch)
        stored_score = meta_qs.get(fingerprint='11')
        self.assertAlmostEquals(stored_score.score, 0.8)
        self.assertEqual(meta_qs.count(), 3)

    def test_state(self):
        b = CassandraBackend(self.manager)
        state = b.states
        state.set_states([r1, r2, r3])
        self.assertEqual([r.meta[b'state'] for r in [r1, r2, r3]], [States.NOT_CRAWLED]*3)
        state.update_cache([r1, r2, r3])
        self.assertDictEqual(state._cache, {b'10': States.NOT_CRAWLED,
                                            b'11': States.NOT_CRAWLED,
                                            b'12': States.NOT_CRAWLED})
        r1.meta[b'state'] = States.CRAWLED
        r2.meta[b'state'] = States.CRAWLED
        r3.meta[b'state'] = States.CRAWLED
        state.update_cache([r1, r2, r3])
        state.flush(True)
        self.assertDictEqual(state._cache, {})
        state.fetch([b'10', b'11', b'12'])
        self.assertDictEqual(state._cache, {b'10': States.CRAWLED,
                                            b'11': States.CRAWLED,
                                            b'12': States.CRAWLED})
        r4.meta[b'state'] = States.ERROR
        state.set_states([r1, r2, r4])
        self.assertEqual(r4.meta[b'state'], States.CRAWLED)
        state.flush(True)
        self.assertEqual(state._cache, {})

    def test_queue(self):
        self.manager.settings.SPIDER_FEED_PARTITIONS = 2
        b = CassandraBackend(self.manager)
        queue = b.queue
        batch = [('10', 0.5, r1, True), ('11', 0.6, r2, True),
                 ('12', 0.7, r3, True)]
        queue.schedule(batch)
        self.assertEqual(set([r.url for r in queue.get_next_requests(10, 0,
                                                                     min_requests=3,
                                                                     min_hosts=1,
                                                                     max_requests_per_host=10)]),
                         set([r3.url]))
        self.assertEqual(set([r.url for r in queue.get_next_requests(10, 1,
                                                                     min_requests=3,
                                                                     min_hosts=1,
                                                                     max_requests_per_host=10)]),
                         set([r1.url, r2.url]))


class BaseCassandraIntegrationTests(object):
    obj = BaseCassandraTest()

    def setup_backend(self, method):
        self.obj.setUp()

    def teardown_backend(self, method):
        self.obj.tearDown()


class TestCassandraFIFOBackend(BaseCassandraIntegrationTests, backends.FIFOBackendTest):
    backend_class = 'frontera.contrib.backends.cassandra.FIFO'


class TestCassandraLIFOBackend(BaseCassandraIntegrationTests, backends.LIFOBackendTest):
    backend_class = 'frontera.contrib.backends.cassandra.LIFO'


class TestCassandraDFSBackend(BaseCassandraIntegrationTests, backends.DFSBackendTest):
    backend_class = 'frontera.contrib.backends.cassandra.DFS'


class TestCassandraBFSBackend(BaseCassandraIntegrationTests, backends.BFSBackendTest):
    backend_class = 'frontera.contrib.backends.cassandra.BFS'
