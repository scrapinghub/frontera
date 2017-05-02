from __future__ import absolute_import
import hashlib
from struct import pack
from binascii import hexlify
from frontera.utils.misc import get_crc32
from frontera.utils.url import parse_url
from w3lib.util import to_bytes


def sha1(key):
    return to_bytes(hashlib.sha1(to_bytes(key, 'utf8')).hexdigest())


def md5(key):
    return to_bytes(hashlib.md5(to_bytes(key, 'utf8')).hexdigest())


def hostname_local_fingerprint(key):
    """
    This function is used for URL fingerprinting, which serves to uniquely identify the document in storage.
    ``hostname_local_fingerprint`` is constructing fingerprint getting first 4 bytes as Crc32 from host, and rest is MD5
    from rest of the URL. Default option is set to make use of HBase block cache. It is expected to fit all the documents
    of average website within one cache block, which can be efficiently read from disk once.

    :param key: str URL
    :return: str 20 bytes hex string
    """
    result = parse_url(key)
    hostname = result.hostname if result.hostname else '-'
    host_checksum = get_crc32(hostname)
    combined = hostname+result.path+';'+result.params+result.query+result.fragment

    combined = to_bytes(combined, 'utf8', 'ignore')
    doc_fprint = hashlib.md5(combined).digest()
    fprint = hexlify(pack(">i16s", host_checksum, doc_fprint))
    return fprint