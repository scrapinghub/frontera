# -*- coding: utf-8 -*-
from __future__ import absolute_import
from frontera.contrib.backends.partitioners import FingerprintPartitioner, Crc32NamePartitioner
from frontera.core.models import Request
from six.moves import range

request = Request('http://www.example.com', meta={b'fingerprint': b'1be68ff556fd0bbe5802d1a100850da29f7f15b1'})

def test_fingerprint_partitioner():
    partitions = list(range(0, 5))
    fp = FingerprintPartitioner(partitions)

    key = b'1be68ff556fd0bbe5802d1a100850da29f7f15b1'
    assert fp.get_key(request) == key

    partition = fp.partition(key, partitions)
    assert partition == 4

    partition = fp.partition(key, None)
    assert partition == 4


def test_crc32name_partitioner():
    partitions = list(range(0, 5))
    cp = Crc32NamePartitioner(partitions)

    key = b'www.example.com'
    assert cp.get_key(request) == key

    partition = cp.partition(key, partitions)
    assert partition == 3

    partition = cp.partition(None, partitions)
    assert partition == 0

    partition = cp.partition(key, None)
    assert partition == 3

