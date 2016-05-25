from frontera.utils.heap import Heap


class TestHeap(object):

    def test_heap_order(self):
        heap = Heap(cmp)
        heap.push(5)
        heap.push(2)
        heap.push(3)
        heap.push(4)
        heap.push(1)
        assert heap.pop(1) == [1]
        assert heap.pop(3) == [2, 3, 4]
        assert heap.pop(10) == [5]
        assert heap.pop(1) == []

    def test_heap_obj(self):
        obj = type('obj', (object,), {})
        a = obj()
        a.score = 3
        b = obj()
        b.score = 1
        c = obj()
        c.score = 2
        heap = Heap(lambda x, y: cmp(x.score, y.score))
        heap.push(a)
        heap.push(b)
        heap.push(c)
        assert heap.pop(3) == [b, c, a]
        assert heap.pop(1) == []
