# -*- coding: utf-8 -*-
from __future__ import absolute_import
from struct import unpack
from binascii import unhexlify

from frontera.core.components import Partitioner
from frontera.utils.misc import get_crc32
from frontera.utils.url import parse_domain_from_url_fast


class Crc32NamePartitioner(Partitioner):
    def partition(self, key, partitions=None):
        if not partitions:
            partitions = self.partitions
        if key is None:
            return partitions[0]
        elif type(key) == int:
            value = key
        else:
            value = get_crc32(key)
        return self.partition_by_hash(value, partitions)

    def partition_by_hash(self, value, partitions):
        size = len(partitions)
        idx = value % size
        return partitions[idx]

    @staticmethod
    def get_key(request):
        domain = request.meta.get(b'domain')
        if domain is not None:
            if type(domain) == dict:
                return domain[b'name']
            elif type(domain) == int:
                return domain
            else:
                raise TypeError("domain of unknown type.")

        try:
            _, name, _, _, _, _ = parse_domain_from_url_fast(request.url)
        except Exception as e:
            return None
        else:
            return name.encode('utf-8', 'ignore')


class FingerprintPartitioner(Partitioner):
    def partition(self, key, partitions=None):
        if not partitions:
            partitions = self.partitions
        digest = unhexlify(key[0:2] + key[5:7] + key[10:12] + key[15:17])
        value = unpack("<I", digest)
        idx = value[0] % len(partitions)
        return partitions[idx]

    @staticmethod
    def get_key(request):
        return request.meta[b'fingerprint']
