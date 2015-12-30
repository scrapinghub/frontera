# -*- coding: utf-8 -*-
from zlib import crc32
from struct import unpack
from binascii import unhexlify

from frontera.core.components import Partitioner


class Crc32NamePartitioner(Partitioner):
    def partition(self, key, partitions=None):
        if key is None:
            return self.partitions[0]
        value = crc32(key) if type(key) is str else crc32(key.encode('utf-8', 'ignore'))
        return self.partition_by_hash(value, partitions if partitions else self.partitions)

    def partition_by_hash(self, value, partitions):
        size = len(partitions)
        idx = value % size
        return partitions[idx]


class FingerprintPartitioner(Partitioner):
    def partition(self, key, partitions=None):
        if not partitions:
            partitions = self.partitions
        digest = unhexlify(key[0:2] + key[5:7] + key[10:12] + key[15:17])
        value = unpack("<I", digest)
        idx = value[0] % len(partitions)
        return partitions[idx]