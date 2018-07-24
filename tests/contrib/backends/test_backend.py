import six
from abc import ABCMeta, abstractmethod
from frontera.core.models import Request, Response
from frontera.core.components import States


r1 = Request('https://www.example.com', meta={b'fingerprint': b'10',
             b'domain': {b'name': b'www.example.com', b'fingerprint': b'81'}})
r2 = Request('http://example.com/some/page/', meta={b'fingerprint': b'11',
             b'domain': {b'name': b'example.com', b'fingerprint': b'82'}})
r3 = Request('http://www.scrapy.org', meta={b'fingerprint': b'12',
             b'domain': {b'name': b'www.scrapy.org', b'fingerprint': b'83'}})
r4 = r3.copy()


@six.add_metaclass(ABCMeta)
class StatesTester(object):

    @abstractmethod
    def get_backend(self):
        pass

    def test_states(self):
        states = self.get_backend().states
        states.set_states([r1, r2, r3])
        assert [r.meta[b'state'] for r in [r1, r2, r3]] == [States.NOT_CRAWLED]*3
        states.update_cache([r1, r2, r3])
        states.flush()

        r1.meta[b'state'] = States.CRAWLED
        r2.meta[b'state'] = States.ERROR
        r3.meta[b'state'] = States.QUEUED
        states.update_cache([r1, r2, r3])
        states.flush()

        r1.meta[b'state'] = States.NOT_CRAWLED
        r2.meta[b'state'] = States.NOT_CRAWLED
        r3.meta[b'state'] = States.NOT_CRAWLED

        states.fetch([b'83'])
        states.set_states([r1, r2, r4])
        assert r4.meta[b'state'] == States.QUEUED
        assert r1.meta[b'state'] == States.CRAWLED
        assert r2.meta[b'state'] == States.ERROR