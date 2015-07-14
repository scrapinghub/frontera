import hashlib
from urlparse import urlparse
from zlib import crc32
from struct import pack
from binascii import hexlify


def sha1(key):
    return hashlib.sha1(key.encode('utf8')).hexdigest()


def md5(key):
    return hashlib.md5(key.encode('utf8')).hexdigest()


def hostname_local_fingerprint(key):
    result = urlparse(key)
    if not result.hostname:
        return sha1(key)
    host_checksum = crc32(result.hostname) if type(result.hostname) is str else \
        crc32(result.hostname.encode('utf-8', 'ignore'))
    doc_uri_combined = result.path+';'+result.params+result.query+result.fragment

    doc_uri_combined = doc_uri_combined if type(doc_uri_combined) is str else \
        doc_uri_combined.encode('utf-8', 'ignore')
    doc_fprint = hashlib.md5(doc_uri_combined).digest()
    fprint = hexlify(pack(">i16s", host_checksum, doc_fprint))
    return fprint