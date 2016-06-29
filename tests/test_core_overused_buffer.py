from frontera.core import OverusedBuffer
from frontera.core.models import Request


r1 = Request('http://www.example.com')
r2 = Request('http://www.example.com/some/')
r3 = Request('htttp://www.example.com/some/page/')
r4 = Request('http://example.com')
r5 = Request('http://example.com/some/page')
r6 = Request('http://example1.com')


class TestOverusedBuffer(object):

    requests = []
    logs = []

    def get_func(self, max_n_requests, **kwargs):
        lst = []
        for _ in range(max_n_requests):
            if self.requests:
                lst.append(self.requests.pop())
        return lst

    def log_func(self, msg):
        self.logs.append(msg)

    def test(self):
        ob = OverusedBuffer(self.get_func, self.log_func)
        self.requests = [r1, r2, r3, r4, r5, r6]
        assert set(ob.get_next_requests(10, overused_keys=['www.example.com', 'example1.com'],
                                        key_type='domain')) == set([r4, r5])
        assert set(self.logs) == set(["Overused keys: ['www.example.com', 'example1.com']",
                                      "Pending: 0"])
        self.logs = []

        assert ob.get_next_requests(10, overused_keys=['www.example.com'],
                                    key_type='domain') == [r6]
        assert set(self.logs) == set(["Overused keys: ['www.example.com']",
                                     "Pending: 4"])
        self.logs = []

        assert ob.get_next_requests(10, overused_keys=['www.example.com'],
                                    key_type='domain') == []
        assert set(self.logs) == set(["Overused keys: ['www.example.com']",
                                      "Pending: 3"])
        self.logs = []

        #the max_next_requests is 3 here to cover the "len(requests) == max_next_requests" case.
        assert set(ob.get_next_requests(3, overused_keys=['example.com'],
                                        key_type='domain')) == set([r1, r2, r3])
        assert set(self.logs) == set(["Overused keys: ['example.com']",
                                      "Pending: 3"])
        self.logs = []

        assert ob.get_next_requests(10, overused_keys=[], key_type='domain') == []
        assert set(self.logs) == set(["Overused keys: []", "Pending: 0"])
