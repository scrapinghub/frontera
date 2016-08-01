from happybase import Connection
from frontera.contrib.backends.hbase import HBaseState, HBaseMetadata, HBaseQueue
from frontera.core.models import Request, Response


r1 = Request('https://www.example.com', meta={'fingerprint': '10',
             'domain': {'name': 'www.example.com', 'fingerprint': '81'}})
r2 = Request('http://example.com/some/page/', meta={'fingerprint': '11',
             'domain': {'name': 'example.com', 'fingerprint': '82'}})
r3 = Request('http://www.scrapy.org', meta={'fingerprint': '12',
             'domain': {'name': 'www.scrapy.org', 'fingerprint': '83'}})
r4 = r3.copy()


class TestHBaseBackend(object):

    def test_metadata(object):
        connection = Connection(host='hbase-docker', port=9090)
        metadata = HBaseMetadata(connection, 'metadata', True, False, 300000, True)
        metadata.add_seeds([r1, r2, r3])
        resp = Response('https://www.example.com', request=r1)
        metadata.page_crawled(resp, [r2, r3])
        metadata.request_error(r4, 'error')
        metadata.frontier_stop()

    def test_queue(object):
        connection = Connection(host='hbase-docker', port=9090)
        queue = HBaseQueue(connection, 1, 'queue', True)
        batch = [('10', 0.5, r1, True), ('11', 0.6, r2, True),
                 ('12', 0.7, r3, True)]
        queue.schedule(batch)
        assert set([r.url for r in queue.get_next_requests(10, 0, min_requests=3, min_hosts=1,
                   max_requests_per_host=10)]) == set([r1.url, r2.url, r3.url])

    def test_state(self):
        connection = Connection(host='hbase-docker', port=9090)
        state = HBaseState(connection, 'metadata', 300000)
        state.set_states([r1, r2, r3])
        assert set([r.meta['state'] for r in [r1, r2, r3]]) == set([0, 0, 0])
        state.update_cache([r1, r2, r3])
        assert state._state_cache == {'10': 0, '11': 0, '12': 0}
        r1.meta['state'] = 2
        r2.meta['state'] = 2
        r3.meta['state'] = 2
        state.update_cache([r1, r2, r3])
        state.flush(True)
        assert state._state_cache == {}
        state.fetch(['10', '11', '12'])
        assert state._state_cache == {'10': 2, '11': 2, '12': 2}
        r4.meta['state'] = 3
        state.set_states([r1, r2, r4])
        assert r4.meta['state'] == 2
        state.flush(True)
        assert state._state_cache == {}
