from calendar import timegm
from datetime import datetime
from struct import pack, unpack

import six

from w3lib.util import to_bytes


_pack_functions = {
    'url': to_bytes,
    'depth': lambda x: pack('>I', 0),
    'created_at': lambda x: pack('>Q', x),
    'status_code': lambda x: pack('>H', x),
    'state': lambda x: pack('>B', x),
    'error': to_bytes,
    'domain_fingerprint': to_bytes,
    'score': lambda x: pack('>f', x),
    'content': to_bytes
}


def unpack_score(blob):
    return unpack(">d", blob)[0]


def prepare_hbase_object(obj=None, **kwargs):
    if not obj:
        obj = dict()
    for k, v in six.iteritems(kwargs):
        if k in ['score', 'state']:
            cf = 's'
        elif k == 'content':
            cf = 'c'
        else:
            cf = 'm'
        func = _pack_functions[k]
        obj[cf + ':' + k] = func(v)
    return obj


def utcnow_timestamp():
    d = datetime.utcnow()
    return timegm(d.timetuple())
