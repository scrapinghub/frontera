import heapq
import math
from cStringIO import StringIO


def show_tree(tree, total_width=80, fill=' '):
    """Pretty-print a tree."""
    print '-' * total_width
    output = StringIO()
    last_row = -1
    for i, n in enumerate(tree):
        if i:
            row = int(math.floor(math.log(i+1, 2)))
        else:
            row = 0
        if row != last_row:
            output.write('\n')
        columns = 2**row
        col_width = int(math.floor((total_width * 1.0) / columns))
        output.write(str(n).center(col_width, fill))
        last_row = row
    print output.getvalue()
    print '-' * total_width
    print
    return


class HeapObjectWrapper(object):
    def __init__(self, obj, compare_function):
        self.obj = obj
        self.compare_function = compare_function

    def __cmp__(self, other):
        return self.compare_function(self.obj, other.obj)

    def __repr__(self):
        return repr(self.obj)

    def __str__(self):
        return str(self.obj)

# When limit is specified, it is a bounded heap and push will use heapreplace and return poped element
# Ignoring the fact that the returned element should actually never be discarded
class Heap(object):
    def __init__(self, compare_function,limit=0):
        self.heap = []
        self._compare_function = compare_function
        self.limit = limit

    def __len__(self):
        return self.heap.__len__()

    def push(self, obj):
        wrapped = HeapObjectWrapper(obj, self._compare_function)
        if self.__len__() >= self.limit > 0:
            return heapq.heapreplace(self.heap,wrapped).obj
        else:
            heapq.heappush(self.heap, wrapped)

    def pop(self, n):
        pages = []
        page = self._extract_object()
        while page:
            pages.append(page)
            if n and len(pages) >= n:
                break
            else:
                page = self._extract_object()
        return pages

    def _extract_object(self):
        try:
            wrapper = heapq.heappop(self.heap)
            return wrapper.obj
        except IndexError:
            return None
