from __future__ import absolute_import
from frontera.core import LegacyOverusedBuffer, OverusedBuffer
from frontera.core.models import Request
from six.moves import range

from tests import mock

from pytest import fixture

sentinel = mock.sentinel
r1 = Request('http://www.example.com')
r2 = Request('http://www.example.com/some/')
r3 = Request('htttp://www.example.com/some/page/')
r4 = Request('http://example.com')
r5 = Request('http://example.com/some/page')
r6 = Request('http://example1.com')


class TestLegacyOverusedBuffer(object):

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
        ob = LegacyOverusedBuffer(self.get_func, self.log_func)
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


@fixture
def fixt_buffer():
    return OverusedBuffer()


@fixture
def fixt_buffer_filled():
    buffer = OverusedBuffer()

    buffer.push_mapping({
        'test1': [sentinel.item1, sentinel.item2, sentinel.item3],
        'test2': [sentinel.item4, sentinel.item5],
        'test3': [sentinel.item6],
    })

    return buffer


def test_fixt_buffer(fixt_buffer):
    assert fixt_buffer.pending == 0


def test_fixt_buffer_filled(fixt_buffer_filled):
    assert fixt_buffer_filled.pending == 6


def test_overused_buffer_pull_all(fixt_buffer_filled):
    assert set(fixt_buffer_filled.pull(-1)) == set([
        sentinel.item1,
        sentinel.item4,
        sentinel.item6,
        sentinel.item2,
        sentinel.item5,
        sentinel.item3,
    ])
    assert fixt_buffer_filled.pending == 0


def test_overused_buffer_pull_n(fixt_buffer_filled):
    assert set(fixt_buffer_filled.pull(4)) == set([
        sentinel.item1,
        sentinel.item4,
        sentinel.item6,
        sentinel.item2,
        sentinel.item5,  # XXX: should we enforce max number?
    ])
    assert fixt_buffer_filled.pending == 1


def test_overused_buffer_pull_with_overused(fixt_buffer_filled):
    assert set(fixt_buffer_filled.pull(-1, {'test1'})) == set([
        sentinel.item4,
        sentinel.item6,
        sentinel.item5,
    ])
    assert fixt_buffer_filled.pending == 3


def test_overused_buffer_push(fixt_buffer):
    fixt_buffer.push('test', sentinel.item)

    assert fixt_buffer.pending == 1
    assert set(fixt_buffer.pull(-1)) == set([sentinel.item])


def test_overused_buffer_push_multiple(fixt_buffer):
    fixt_buffer.push_multiple('test', [sentinel.item1, sentinel.item2])

    assert fixt_buffer.pending == 2
    assert set(fixt_buffer.pull(-1)) == set([sentinel.item1, sentinel.item2])


def test_overused_buffer_push_mapping(fixt_buffer):
    fixt_buffer.push_mapping({
        'test1': [sentinel.item1, sentinel.item2],
        'test2': [sentinel.item3],
    })

    assert fixt_buffer.pending == 3
    assert set(fixt_buffer.pull(2)) == set([
        sentinel.item1,
        sentinel.item3,
    ])
    assert set(fixt_buffer.pull(-1)) == set([
        sentinel.item2,
    ])


def test_overused_buffer_push_sequence(fixt_buffer):
    def key_function(item):
        return item.name.split('_')[0]

    fixt_buffer.push_sequence([
        sentinel.test1_item1,
        sentinel.test1_item2,
        sentinel.test1_item3,
        sentinel.test2_item4,
        sentinel.test2_item5,
        sentinel.test3_item6,
    ], key_function)

    assert fixt_buffer.pending == 6
    assert set(fixt_buffer.pull(3)) == set([
        sentinel.test1_item1,
        sentinel.test2_item4,
        sentinel.test3_item6,
    ])
    assert set(fixt_buffer.pull(2)) == set([
        sentinel.test1_item2,
        sentinel.test2_item5,
    ])
    assert set(fixt_buffer.pull(-1)) == set([
        sentinel.test1_item3,
    ])
