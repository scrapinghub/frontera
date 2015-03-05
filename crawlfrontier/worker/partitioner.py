# -*- coding: utf-8 -*-
from struct import unpack
from kafka.partitioner.base import Partitioner


class FingerprintPartitioner(Partitioner):
    def partition(self, key, partitions):
        size = len(partitions)
        value = unpack("<I", key[:4])
        idx = value[0] % size
        return partitions[idx]