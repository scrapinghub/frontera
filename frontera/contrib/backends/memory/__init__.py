from __future__ import absolute_import

from .backend import (
    MemoryBFSBackend,
    MemoryBaseBackend,
    MemoryDFSBackend,
    MemoryDFSOverusedBackend,
    MemoryFIFOBackend,
    MemoryLIFOBackend,
    MemoryRandomBackend,
)


from .dequequeue import MemoryDequeQueue
from .metadata import MemoryMetadata
from .queue import (
    MemoryBFSQueue,
    MemoryDFSQueue,
    MemoryQueue,
    MemoryRandomQueue,
)
from .states import MemoryStates


BASE = MemoryBaseBackend
FIFO = MemoryFIFOBackend
LIFO = MemoryLIFOBackend
DFS = MemoryDFSBackend
BFS = MemoryBFSBackend
RANDOM = MemoryRandomBackend


__all__ = [
    'BASE', 'BFS', 'DFS', 'FIFO', 'LIFO', 'MemoryBaseBackend',
    'MemoryBaseBackend', 'MemoryBFSBackend', 'MemoryBFSBackend',
    'MemoryBFSQueue', 'MemoryDequeQueue', 'MemoryDFSBackend',
    'MemoryDFSBackend', 'MemoryDFSOverusedBackend', 'MemoryDFSQueue',
    'MemoryFIFOBackend', 'MemoryFIFOBackend', 'MemoryLIFOBackend',
    'MemoryLIFOBackend', 'MemoryMetadata', 'MemoryQueue',
    'MemoryRandomBackend', 'MemoryRandomBackend', 'MemoryRandomQueue',
    'MemoryStates', 'RANDOM',
]
