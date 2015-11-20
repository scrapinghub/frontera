# -*- coding: utf-8 -*-
from core.components import Partitioner
from zlib import crc32


class Crc32NamePartitioner(Partitioner):
    def partition(self, key, partitions=None):
        value = crc32(key) if type(key) is str else crc32(key.encode('utf-8', 'ignore'))
        return self.partition_by_hash(value, partitions if partitions else self.partitions)

    def partition_by_hash(self, value, partitions):
        size = len(partitions)
        idx = value % size
        return partitions[idx]