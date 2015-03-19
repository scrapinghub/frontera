# -*- coding: utf-8 -*-
from struct import unpack
from binascii import unhexlify
from kafka.partitioner.base import Partitioner


class FingerprintPartitioner(Partitioner):
    def partition(self, key, partitions):
        size = len(partitions)
        digest = unhexlify(key[0:2] + key[5:7] + key[10:12] + key[15:17])
        value = unpack("<I", digest)
        idx = value[0] % size
        return partitions[idx]