# -*- coding: utf-8 -*-
from __future__ import absolute_import

from frontera.core.components import Partitioner
from cityhash import CityHash64
from frontera.utils.misc import get_crc32


class Crc32NamePartitioner(Partitioner):
    def partition(self, key, partitions=None):
        if key is None:
            return self.partitions[0]
        value = get_crc32(key)
        return self.partition_by_hash(value, partitions if partitions else self.partitions)

    def partition_by_hash(self, value, partitions):
        size = len(partitions)
        idx = value % size
        return partitions[idx]

    def __call__(self, key, all_partitions, available):
        return self.partition(key, all_partitions)


class FingerprintPartitioner(Partitioner):
    def partition(self, key, partitions=None):
        if not partitions:
            partitions = self.partitions
        value = CityHash64(key)
        idx = value % len(partitions)
        return partitions[idx]

    def __call__(self, key, all_partitions, available):
        return self.partition(key, all_partitions)