# -*- coding: utf-8 -*-
import uuid
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model


class MetadataModel(Model):
    __table_name__ = 'metadata'

    meta_id = columns.UUID(primary_key=True, default=uuid.uuid4)
    fingerprint = columns.Text(primary_key=True)
    url = columns.Text(index=True)
    depth = columns.Integer()
    created_at = columns.DateTime()
    fetched_at = columns.DateTime(required=False)
    status_code = columns.Text(required=False)
    score = columns.Float(required=False)
    error = columns.Text(required=False)
    meta = columns.Map(required=False)
    headers = columns.Map(required=False)
    cookies = columns.Map(required=False)
    method = columns.Text(required=False)

    @classmethod
    def query(cls, session):
        return session.query(cls)

    def __repr__(self):
        return '<Metadata:%s (%s)>' % (self.url, self.fingerprint)


class StateModel(Model):
    __table_name__ = 'states'

    state_id = columns.UUID(primary_key=True, default=uuid.uuid4)
    fingerprint = columns.Text(primary_key=True)
    state = columns.SmallInt()

    @classmethod
    def query(cls, session):
        return session.query(cls)

    def __repr__(self):
        return '<State:%s=%d>' % (self.fingerprint, self.state)


class QueueModelMixin(object):
    id = columns.UUID(primary_key=True, default=uuid.uuid4)
    partition_id = columns.Integer(index=True)
    score = columns.Float(index=True)
    url = columns.Text()
    fingerprint = columns.Text()
    host_crc32 = columns.Integer()
    meta = columns.Map(required=False)
    headers = columns.Map(required=False)
    cookies = columns.Map(required=False)
    method = columns.Text(required=False)
    created_at = columns.BigInt(index=True, required=False)
    depth = columns.SmallInt(required=False)


class QueueModel(QueueModelMixin, Model):
    __table_name__ = 'queue'

    @classmethod
    def query(cls, session):
        return session.query(cls)

    def __repr__(self):
        return '<Queue:%s (%d)>' % (self.url, self.id)
