from __future__ import absolute_import
from happybase import Connection
from frontera.contrib.backends.hbase import HBaseState, HBaseMetadata, HBaseQueue
from frontera.core.models import Request, Response
from frontera.core.components import States
from binascii import unhexlify
from time import sleep, time

r1 = Request('https://www.example.com', meta={'fingerprint': '10',
             'domain': {'name': 'www.example.com', 'fingerprint': '81'}})
r2 = Request('http://example.com/some/page/', meta={'fingerprint': '11',
             'domain': {'name': 'example.com', 'fingerprint': '82'}})
r3 = Request('http://www.scrapy.org', meta={'fingerprint': '12',
             'domain': {'name': 'www.scrapy.org', 'fingerprint': '83'}})
r4 = r3.copy()



class TestHBaseBackend(object):

    def delete_rows(self, table, row_keys):
        batch = table.batch()
        for key in row_keys:
            batch.delete(unhexlify(key))
        batch.send()

    def test_metadata(self):
        connection = Connection(host='hbase-docker', port=9090)
        metadata = HBaseMetadata(connection, 'metadata', True, False, 300000, True)
        metadata.add_seeds([r1, r2, r3])
        resp = Response('https://www.example.com', request=r1)
        metadata.page_crawled(resp, [r2, r3])
        metadata.request_error(r4, 'error')
        metadata.frontier_stop()
        table = connection.table('metadata')
        assert set([data['m:url'] for _, data in table.scan()]) == \
            set([r1.url, r2.url, r3.url])
        self.delete_rows(table, ['10', '11', '12'])

    def test_queue(self):
        connection = Connection(host='hbase-docker', port=9090)
        queue = HBaseQueue(connection, 2, 'queue', True)
        batch = [('10', 0.5, r1, True), ('11', 0.6, r2, True),
                 ('12', 0.7, r3, True)]
        queue.schedule(batch)
        assert set([r.url for r in queue.get_next_requests(10, 0, min_requests=3, min_hosts=1,
                   max_requests_per_host=10)]) == set([r3.url])
        assert set([r.url for r in queue.get_next_requests(10, 1, min_requests=3, min_hosts=1,
                   max_requests_per_host=10)]) == set([r1.url, r2.url])

    def test_queue_with_delay(self):
        connection = Connection(host='hbase-docker', port=9090)
        queue = HBaseQueue(connection, 1, 'queue', True)
        r5 = r3.copy()
        r5.meta['crawl_at'] = int(time()) + 1
        batch = [(r5.meta['fingerprint'], 0.5, r5, True)]
        queue.schedule(batch)
        assert queue.get_next_requests(10, 0, min_requests=3, min_hosts=1,
                   max_requests_per_host=10) == []
        sleep(1.0)
        assert set([r.url for r in queue.get_next_requests(10, 0, min_requests=3, min_hosts=1,
                   max_requests_per_host=10)]) == set([r5.url])

    def test_state(self):
        connection = Connection(host='hbase-docker', port=9090)
        state = HBaseState(connection, 'metadata', 300000)
        state.set_states([r1, r2, r3])
        assert [r.meta['state'] for r in [r1, r2, r3]] == [States.NOT_CRAWLED]*3
        state.update_cache([r1, r2, r3])
        assert state._state_cache == {'10': States.NOT_CRAWLED,
                                      '11': States.NOT_CRAWLED,
                                      '12': States.NOT_CRAWLED}
        r1.meta['state'] = States.CRAWLED
        r2.meta['state'] = States.CRAWLED
        r3.meta['state'] = States.CRAWLED
        state.update_cache([r1, r2, r3])
        state.flush(True)
        assert state._state_cache == {}
        state.fetch(['10', '11', '12'])
        assert state._state_cache == {'10': States.CRAWLED,
                                      '11': States.CRAWLED,
                                      '12': States.CRAWLED}
        r4.meta['state'] = States.ERROR
        state.set_states([r1, r2, r4])
        assert r4.meta['state'] == States.CRAWLED
        state.flush(True)
        assert state._state_cache == {}
