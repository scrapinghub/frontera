from __future__ import absolute_import
from frontera.core import OverusedBuffer
from frontera.core.models import Request
from six.moves import range
from itertools import cycle
from random import choice, sample
from string import ascii_lowercase


r1 = Request('http://www.example.com')
r2 = Request('http://www.example.com/some/')
r3 = Request('htttp://www.example.com/some/page/')
r4 = Request('http://example.com')
r5 = Request('http://example.com/some/page')
r6 = Request('http://example1.com')


class TestOverusedBuffer(object):

    requests = [r1, r2, r3, r4, r5, r6]

    def get_once(self, max_n_requests, **kwargs):
        lst = []
        for _ in range(max_n_requests):
            try:
                lst.append(next(self.req_it))
            except StopIteration:
                break
        return lst

    def test_base(self):
        self.req_it = iter(self.requests)
        ob = OverusedBuffer(self.get_once, 100, 10000)

        assert ob._get_pending_count() == 0
        assert set(ob.get_next_requests(10, overused_keys=['www.example.com', 'example1.com'],
                                        key_type='domain')) == set([r4, r5])
        assert ob._get_pending_count() == 4
        assert ob.get_next_requests(10, overused_keys=['www.example.com'],
                                    key_type='domain') == [r6]
        assert ob._get_pending_count() == 3

        assert ob.get_next_requests(10, overused_keys=['www.example.com'],
                                    key_type='domain') == []
        assert ob._get_pending_count() == 3

        #the max_next_requests is 3 here to cover the "len(requests) == max_next_requests" case.
        assert set(ob.get_next_requests(3, overused_keys=['example.com'],
                                        key_type='domain')) == set([r1, r2, r3])
        assert ob._get_pending_count() == 0

        assert ob.get_next_requests(10, overused_keys=[], key_type='domain') == []
        assert ob._get_pending_count() == 0

    def test_purging_keys(self):
        self.req_it = cycle(self.requests)
        ob = OverusedBuffer(self.get_once, 10, 100)
        ob.get_next_requests(10, overused_keys=["example.com", "www.example.com"],
                             key_type="domain")
        assert ob._get_pending_count() == 9
        ob.get_next_requests(10, overused_keys=["example.com", "www.example.com"],
                             key_type="domain") # purging of www.example.com
        assert ob._get_pending_count() == 7

    def generate_requests(self):
        def get_random_host():
            return str("").join([choice(ascii_lowercase) for i in range(5)])

        self.hosts = set()
        for _ in range(21):
            self.hosts.add(get_random_host())
        self.requests = []
        for host in self.hosts:
            self.requests.append(Request("http://%s/" % (host)))


    def test_purging_keys_set(self):
        self.generate_requests()
        self.req_it = cycle(self.requests)
        ob = OverusedBuffer(self.get_once, 1000, 10)

        ob.get_next_requests(10, overused_keys=self.hosts, key_type="domain")
        assert (ob._get_key_count()) == 10

        ob.get_next_requests(10, overused_keys=self.hosts, key_type="domain")
        assert (ob._get_key_count()) == 20

        ob.get_next_requests(10, overused_keys=self.hosts, key_type="domain")   # purging of keys set
        assert (ob._get_key_count()) < 20
