from __future__ import absolute_import
from unittest import TestCase, main

from frontera.contrib.backends.memory import MemoryQueue
from tests.test_overused_buffer import DFSOverusedBackendTest
from tests import backends
from frontera.core.models import Request

class TestFIFO(backends.FIFOBackendTest):
    backend_class = 'frontera.contrib.backends.memory.FIFO'


class TestLIFO(backends.LIFOBackendTest):
    backend_class = 'frontera.contrib.backends.memory.LIFO'


class TestDFS(backends.DFSBackendTest):
    backend_class = 'frontera.contrib.backends.memory.DFS'


class TestDFSOverused(backends.DFSBackendTest):
    backend_class = 'frontera.contrib.backends.memory.MemoryDFSOverusedBackend'


class TestDFSOverusedSimulation(DFSOverusedBackendTest):
    backend_class = 'frontera.contrib.backends.memory.MemoryDFSOverusedBackend'


class TestBFS(backends.BFSBackendTest):
    backend_class = 'frontera.contrib.backends.memory.BFS'


class TestRANDOM(backends.RANDOMBackendTest):
    backend_class = 'frontera.contrib.backends.memory.RANDOM'


class TestMemoryQueue(TestCase):
    def test_scheduling_past_1part_post(self):
        subject = MemoryQueue(1)
        data={'id':'xxx',
              'name':'yyy'}
        batch = [
            ("1", 1, Request(url='https://www.knuthellan.com/', body=data, method='POST'), True),
        ]
        subject.schedule(batch)
        requests = subject.get_next_requests(5, 0)
        for request in requests:
            self.assertTrue(request.method == b'POST')
            self.assertTrue(request.body == data)

