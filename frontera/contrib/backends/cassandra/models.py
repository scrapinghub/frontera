# -*- coding: utf-8 -*-
import pickle
import six

from cassandra.cqlengine.columns import (UUID, BigInt, Bytes, DateTime, Float,
                                         Integer, Map, SmallInt, Text)
from cassandra.cqlengine.models import Model


class Pickle(Bytes):

    def to_database(self, value):
        value = self._pickle_object(value)
        return super(Pickle, self).to_database(value)

    def to_python(self, value):
        value = super(Pickle, self).to_python(value)
        return self._unpickle_object(value)

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
    meta = Pickle()
    headers = Pickle()
    cookies = Pickle()
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
    partition_id = Integer(required=True)
    score = Float(required=True)
    url = Text(required=True)
    fingerprint = Text(required=True)
    host_crc32 = Integer(required=True)
    meta = Pickle()
    headers = Pickle()
    cookies = Pickle()
    method = Text()
    created_at = BigInt(required=True)
    depth = SmallInt()

    def __repr__(self):
        return '<Queue:%s (%s)>' % (self.url, self.id)