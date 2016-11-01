import unittest
import six
import uuid
from datetime import datetime

from cassandra.cluster import Cluster
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import drop_table, sync_table

from frontera.contrib.backends.cassandra import CassandraBackend
from frontera.contrib.backends.cassandra.models import MetadataModel, StateModel, QueueModel
from frontera.settings import Settings


class BaseCassendraTest(unittest.TestCase):
    def setUp(self):
        settings = Settings()
        hosts = ['127.0.0.1']
        port = 9042
        self.manager = type('manager', (object,), {})
        self.manager.settings = settings
        self.keyspace = settings.CASSANDRABACKEND_KEYSPACE
        cluster = Cluster(hosts, port, control_connection_timeout=240)
        self.session = cluster.connect()
        self.session.execute("CREATE KEYSPACE IF NOT EXISTS %s WITH "
                             "replication = {'class':'SimpleStrategy', 'replication_factor' : 3}" % self.keyspace)
        connection.setup(hosts, self.keyspace, port=port, control_connection_timeout=240)
        self.session.set_keyspace(self.keyspace)

    def tearDown(self):
        tables = self._get_tables()
        models = [MetadataModel, StateModel, QueueModel]
        for model in models:
            if model.__table_name__ in tables:
                # self.session.execute('DROP TABLE {0};'.format(model.column_family_name()), timeout=240)
                drop_table(model)
        self.session.shutdown()

    def _get_tables(self):
        query = self.session.prepare('SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?')
        result = self.session.execute(query, (self.session.keyspace,))
        return [row.table_name for row in result.current_rows]


class TestCassandraBackendModels(BaseCassendraTest):
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
        self.assert_db_values(MetadataModel, 'fingerprint', fields)

    def test_state_model(self):
        fields = {
            'fingerprint': 'fingerprint',
            'state': 1
        }
        self.assert_db_values(StateModel, 'fingerprint', fields)

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
            'created_at': datetime.now(),
            'depth': 0,
        }
        self.assert_db_values(QueueModel, 'id', fields)

    def assert_db_values(self, model, primary_key, fields):
        sync_table(model)
        m = model(**fields)
        m.save()
        stored_obj = m.get(fingerprint=fields[primary_key])
        for field, original_value in six.iteritems(fields):
            stored_value = getattr(stored_obj, field)
            if isinstance(original_value, dict):
                self.assertDictEqual(stored_value, original_value)
            else:
                self.assertEqual(stored_value, original_value)


class TestCassandraBackend(BaseCassendraTest):

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
