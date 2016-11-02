# -*- coding: utf-8 -*-
import pickle
import six

from cassandra.cqlengine.columns import (UUID, BigInt, Bytes, DateTime, Float,
                                         Integer, SmallInt, Text)
from cassandra.cqlengine.models import Model


class PickleDict(Bytes):
    """
    PickleDict applies Python's ``pickle.dumps()`` to incoming objects
    if the value received is a dict, and ``pickle.loads()`` on the way out.
    """

    def to_database(self, value):
        if value is None:
            return
        if isinstance(value, dict):
            value = self._pickle_object(value)
        return super(PickleDict, self).to_database(value)

    def to_python(self, value):
        value = super(PickleDict, self).to_python(value)
        if value is None:
            return
        try:
            return self._unpickle_object(value)
        except TypeError:
            return value

    def _pickle_object(self, obj):
        pickled = pickle.dumps(obj)
        return pickled.encode('hex') if six.PY2 else pickled

    def _unpickle_object(self, pickled_obj):
        obj = pickled_obj.decode('hex') if six.PY2 else pickled_obj
        return pickle.loads(obj)


class MetadataModel(Model):
    __table_name__ = 'metadata'

    fingerprint = Text(primary_key=True)
    url = Text(required=True)
    depth = Integer(required=True)
    created_at = DateTime(required=True)
    fetched_at = DateTime()
    status_code = Integer()
    score = Float()
    error = Text()
    meta = PickleDict()
    headers = PickleDict()
    cookies = PickleDict()
    method = Text()

    def __repr__(self):
        return '<Metadata:%s (%s)>' % (self.url, self.fingerprint)


class StateModel(Model):
    __table_name__ = 'states'

    fingerprint = Text(primary_key=True)
    state = SmallInt(required=True)

    def __repr__(self):
        return '<State:%s=%s>' % (self.fingerprint, self.state)


class QueueModel(Model):
    __table_name__ = 'queue'

    id = UUID(primary_key=True)
    partition_id = Integer(primary_key=True)
    score = Float(required=True)
    url = Text(required=True)
    fingerprint = Text(required=True)
    host_crc32 = Integer(required=True)
    meta = PickleDict()
    headers = PickleDict()
    cookies = PickleDict()
    method = Text()
    created_at = BigInt(required=True)
    depth = SmallInt()

    def __repr__(self):
        return '<Queue:%s (%s)>' % (self.url, self.id)
