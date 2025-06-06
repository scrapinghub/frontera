from binascii import unhexlify
from struct import unpack

from frontera.core.components import Partitioner
from frontera.utils.misc import get_crc32


class Crc32NamePartitioner(Partitioner):
    def partition(self, key, partitions=None):
        if key is None:
            return self.partitions[0]
        value = get_crc32(key)
        return self.partition_by_hash(
            value, partitions if partitions else self.partitions
        )

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
        digest = unhexlify(key[0:2] + key[5:7] + key[10:12] + key[15:17])
        value = unpack("<I", digest)
        idx = value[0] % len(partitions)
        return partitions[idx]

    def __call__(self, key, all_partitions, available):
        return self.partition(key, all_partitions)
