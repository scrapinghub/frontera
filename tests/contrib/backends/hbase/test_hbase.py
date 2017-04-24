from __future__ import absolute_import

from binascii import unhexlify
from time import time

from happybase import Connection

# module loaded at runtime in happybase
from Hbase_thrift import AlreadyExists  # noqa

import pytest

from w3lib.util import to_native_str

from frontera.contrib.backends.hbase import HBaseMetadata, HBaseQueue, HBaseState
from frontera.core.components import States
from frontera.core.models import Request, Response

from tests import mock


PREFIX = 'frontera.contrib.backends.hbase.'


@pytest.fixture
def connection():
    return Connection(host='hbase-docker', port=9090)


@pytest.fixture
def requests():
    return [
        Request(
            'https://www.example.com',
            meta={
                b'fingerprint': b'10',
                b'domain': {
                    b'name': b'www.example.com',
                    b'fingerprint': b'81',
                },
            },
        ),
        Request(
            'http://example.com/some/page/',
            meta={
                b'fingerprint': b'11',
                b'domain': {
                    b'name': b'example.com',
                    b'fingerprint': b'82',
                },
            },
        ),
        Request(
            'http://www.scrapy.org',
            meta={
                b'fingerprint': b'12',
                b'domain': {
                    b'name': b'www.scrapy.org',
                    b'fingerprint': b'83',
                },
            },
        ),
    ]


class TestHBaseBackend(object):

    def delete_rows(self, table, row_keys):
        batch = table.batch()

        for key in row_keys:
            batch.delete(unhexlify(key))

        batch.send()

    def test_metadata(self, connection, requests):
        metadata = HBaseMetadata(
            connection, b'metadata', True, False, 300000, True,
        )

        metadata.add_seeds(requests)

        resp = Response('https://www.example.com', request=requests[0])
        error_request = requests[2].copy()

        metadata.page_crawled(resp)
        metadata.links_extracted(resp.request, requests[1:])
        metadata.request_error(error_request, 'error')
        metadata.frontier_stop()

        table = connection.table('metadata')

        assert {
            to_native_str(data[b'm:url'], 'utf-8')
            for _, data in table.scan()
        } == {
            request.url
            for request in requests
        }

        self.delete_rows(table, [b'10', b'11', b'12'])

    def test_queue(self, connection, requests):
        queue = HBaseQueue(connection, 2, b'queue', True)
        batch = [
            ('10', 0.5, requests[0], True),
            ('11', 0.6, requests[1], True),
            ('12', 0.7, requests[2], True),
        ]

        queue.schedule(batch)

        next_requests = queue.get_next_requests(
            10, 0, min_requests=3, min_hosts=1, max_requests_per_host=10,
        )

        assert {
            next_request.url
            for next_request in next_requests
        } == {
            requests[2].url,
        }

        next_requests = queue.get_next_requests(
            10, 1, min_requests=3, min_hosts=1, max_requests_per_host=10,
        )

        assert {
            next_request.url
            for next_request in next_requests
        } == {
            requests[0].url,
            requests[1].url,
        }

    def test_queue_with_delay(self, connection, requests):
        queue = HBaseQueue(connection, 1, b'queue', True)
        request = requests[2].copy()
        crawl_at = int(time()) + 1000
        request.meta[b'crawl_at'] = crawl_at
        batch = [
            (request.meta[b'fingerprint'], 0.5, request, True),
        ]

        queue.schedule(batch)

        with mock.patch(PREFIX + 'time') as mocked_time:
            mocked_time.return_value = time()

            next_requests = queue.get_next_requests(
                10, 0, min_requests=3, min_hosts=1, max_requests_per_host=10,
            )

            assert next_requests == []

            mocked_time.return_value = crawl_at + 1

            next_requests = queue.get_next_requests(
                10, 0, min_requests=3, min_hosts=1, max_requests_per_host=10,
            )

            assert {
                next_request.url
                for next_request in next_requests
            } == {
                request.url,
            }

    def test_state(self, connection, requests):
        state = HBaseState(connection, b'metadata', 300000)

        state.set_states(requests)

        assert [
            request.meta[b'state']
            for request in requests
        ] == [States.NOT_CRAWLED] * 3

        state.update_cache(requests)

        assert state._state_cache == {
            b'10': States.NOT_CRAWLED,
            b'11': States.NOT_CRAWLED,
            b'12': States.NOT_CRAWLED,
        }

        requests[0].meta[b'state'] = States.CRAWLED
        requests[1].meta[b'state'] = States.CRAWLED
        requests[2].meta[b'state'] = States.CRAWLED

        state.update_cache(requests)
        state.flush(True)

        assert state._state_cache == {}

        state.fetch([b'10', b'11', b'12'])

        assert state._state_cache == {
            b'10': States.CRAWLED,
            b'11': States.CRAWLED,
            b'12': States.CRAWLED,
        }

        error_request = requests[2].copy()
        error_request.meta[b'state'] = States.ERROR

        state.set_states([requests[0], requests[1], error_request])

        assert error_request.meta[b'state'] == States.CRAWLED

        state.flush(True)

        assert state._state_cache == {}

    def test_drop_all_tables_when_table_name_is_str(self, connection):
        for table in connection.tables():
            connection.delete_table(table, True)

        hbase_queue_table = 'queue'
        hbase_metadata_table = 'metadata'

        connection.create_table(hbase_queue_table, {'f': {'max_versions': 1}})
        connection.create_table(hbase_metadata_table, {'f': {'max_versions': 1}})

        tables = connection.tables()

        assert set(tables) == {b'metadata', b'queue'}  # Failure of test itself

        try:
            HBaseQueue(
                connection=connection, partitions=1,
                table_name=hbase_queue_table, drop=True,
            )
            HBaseMetadata(
                connection=connection, table_name=hbase_metadata_table,
                drop_all_tables=True, use_snappy=False, batch_size=300000,
                store_content=True,
            )
        except AlreadyExists:
            assert False, "failed to drop hbase tables"
