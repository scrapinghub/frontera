from __future__ import absolute_import
from happybase import Connection
from Hbase_thrift import AlreadyExists  # module loaded at runtime in happybase
from frontera.contrib.backends.hbase import HBaseState, HBaseMetadata, HBaseQueue
from frontera.core.models import Request, Response
from frontera.core.components import States
from binascii import unhexlify
from time import time
from w3lib.util import to_native_str
from tests import mock
import pytest

r1 = Request('https://www.example.com', meta={b'fingerprint': b'10',
             b'domain': {b'name': b'www.example.com', b'fingerprint': b'81'}})
r2 = Request('http://example.com/some/page/', meta={b'fingerprint': b'11',
             b'domain': {b'name': b'example.com', b'fingerprint': b'82'}})
r3 = Request('http://www.scrapy.org', meta={b'fingerprint': b'12',
             b'domain': {b'name': b'www.scrapy.org', b'fingerprint': b'83'}})
r4 = r3.copy()


class TestHBaseBackend(object):

    def delete_rows(self, table, row_keys):
        batch = table.batch()
        for key in row_keys:
            batch.delete(unhexlify(key))
        batch.send()

    def test_metadata(self):
        connection = Connection(host='hbase-docker', port=9090)
        metadata = HBaseMetadata(connection, b'metadata', True, False, 300000, True)
        metadata.add_seeds([r1, r2, r3])
        resp = Response('https://www.example.com', request=r1)
        metadata.page_crawled(resp)
        metadata.links_extracted(resp.request, [r2, r3])
        metadata.request_error(r4, 'error')
        metadata.frontier_stop()
        table = connection.table('metadata')
        assert set([to_native_str(data[b'm:url'], 'utf-8') for _, data in table.scan()]) == \
            set([r1.url, r2.url, r3.url])
        self.delete_rows(table, [b'10', b'11', b'12'])

    def test_queue(self):
        connection = Connection(host='hbase-docker', port=9090)
        queue = HBaseQueue(connection, 2, b'queue', True)
        batch = [('10', 0.5, r1, True), ('11', 0.6, r2, True),
                 ('12', 0.7, r3, True)]
        queue.schedule(batch)
        assert set([r.url for r in queue.get_next_requests(10, 0, min_requests=3, min_hosts=1,
                   max_requests_per_host=10)]) == set([r3.url])
        assert set([r.url for r in queue.get_next_requests(10, 1, min_requests=3, min_hosts=1,
                   max_requests_per_host=10)]) == set([r1.url, r2.url])

    @pytest.mark.xfail
    def test_queue_with_delay(self):
        connection = Connection(host='hbase-docker', port=9090)
        queue = HBaseQueue(connection, 1, b'queue', True)
        r5 = r3.copy()
        crawl_at = int(time()) + 1000
        r5.meta[b'crawl_at'] = crawl_at
        batch = [(r5.meta[b'fingerprint'], 0.5, r5, True)]
        queue.schedule(batch)
        with mock.patch('frontera.contrib.backends.hbase.time') as mocked_time:
            mocked_time.return_value = time()
            assert queue.get_next_requests(10, 0, min_requests=3, min_hosts=1,
                                           max_requests_per_host=10) == []
            mocked_time.return_value = crawl_at + 1
            assert set([r.url for r in queue.get_next_requests(10, 0, min_requests=3, min_hosts=1,
                       max_requests_per_host=10)]) == set([r5.url])

    def test_state(self):
        connection = Connection(host='hbase-docker', port=9090)
        state = HBaseState(connection, b'metadata', 300000)
        state.set_states([r1, r2, r3])
        assert [r.meta[b'state'] for r in [r1, r2, r3]] == [States.NOT_CRAWLED]*3
        state.update_cache([r1, r2, r3])
        assert state._state_cache == {b'10': States.NOT_CRAWLED,
                                      b'11': States.NOT_CRAWLED,
                                      b'12': States.NOT_CRAWLED}
        r1.meta[b'state'] = States.CRAWLED
        r2.meta[b'state'] = States.CRAWLED
        r3.meta[b'state'] = States.CRAWLED
        state.update_cache([r1, r2, r3])
        state.flush(True)
        assert state._state_cache == {}
        state.fetch([b'10', b'11', b'12'])
        assert state._state_cache == {b'10': States.CRAWLED,
                                      b'11': States.CRAWLED,
                                      b'12': States.CRAWLED}
        r4.meta[b'state'] = States.ERROR
        state.set_states([r1, r2, r4])
        assert r4.meta[b'state'] == States.CRAWLED
        state.flush(True)
        assert state._state_cache == {}

    def test_drop_all_tables_when_table_name_is_str(self):
        connection = Connection(host='hbase-docker', port=9090)
        for table in connection.tables():
            connection.delete_table(table, True)
        hbase_queue_table = 'queue'
        hbase_metadata_table = 'metadata'
        connection.create_table(hbase_queue_table, {'f': {'max_versions': 1}})
        connection.create_table(hbase_metadata_table, {'f': {'max_versions': 1}})
        tables = connection.tables()
        assert set(tables) == set([b'metadata', b'queue'])  # Failure of test itself
        try:
            HBaseQueue(connection=connection, partitions=1, table_name=hbase_queue_table, drop=True)
            HBaseMetadata(connection=connection, table_name=hbase_metadata_table, drop_all_tables=True,
                          use_snappy=False, batch_size=300000, store_content=True)
        except AlreadyExists:
            assert False, "failed to drop hbase tables"
