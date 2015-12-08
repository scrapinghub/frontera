import unittest
from mock import ANY
from frontera.utils.heap import Heap, HeapObjectWrapper
from frontera.core.models import Request

# higher priority =>smaller value, higher in heap
def compare_request(first, second):
    return first.meta.get('priority',0) < second.meta.get('priority',0)

req1 = Request(ANY, meta={'priority':1})
req2 = Request(ANY, meta={'priority':2})
req3 = Request(ANY, meta={'priority':3})


class HeapTest(unittest.TestCase):

    def setUp(self):
        self.heap = Heap(compare_request)

    def test_push(self):
        self.heap.push(req1)
        self.heap.push(req2)
        assert len(self.heap) == 2

    def test_pop0_as_pop0(self):
        print len(self.heap)
        self.heap.push(req1)
        req = self.heap.pop(0)
        assert len(req) == 1
        assert req[0] == req1
        self.heap.push(req1)
        req = self.heap.pop(1)
        assert len(req) == 1
        assert req[0] == req1

    def test_pop_smallest_in_heap(self):
        self.heap.push(req1)
        self.heap.push(req2)
        req = self.heap.pop(0)
        assert req[0] == req2

    def test_push_limit(self):
        self.heap = Heap(compare_request,limit=2)
        self.heap.push(req1)
        self.heap.push(req2)
        p = self.heap.push(req3)
        assert p == req2
        assert len(self.heap) == 2
        assert self.heap.pop(2) == [req3,req1]
