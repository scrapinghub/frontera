# -*- coding: utf-8 -*-
import uuid
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model


class MetadataModel(Model):
    __table_name__ = 'metadata'

    fingerprint = columns.Text(primary_key=True)
    url = columns.Text(index=True)
    depth = columns.Integer()
    created_at = columns.DateTime()
    fetched_at = columns.DateTime(required=False)
    status_code = columns.Integer(required=False)
    score = columns.Float(required=False)
    error = columns.Text(required=False)
    meta = columns.Map(columns.Text(), columns.Text(), required=False)
    headers = columns.Map(columns.Text(), columns.Text(), required=False)
    cookies = columns.Map(columns.Text(), columns.Text(), required=False)
    method = columns.Text(required=False)

    @classmethod
    def query(cls, session):
        return session.query(cls)

    def __repr__(self):
        return '<Metadata:%s (%s)>' % (self.url, self.fingerprint)


class StateModel(Model):
    __table_name__ = 'states'

    state_id = columns.UUID(primary_key=True, default=uuid.uuid4)
    fingerprint = columns.Text(primary_key=True, index=True)
    state = columns.SmallInt(index=True)

    @classmethod
    def query(cls, session):
        return session.query(cls)

    def __repr__(self):
        return '<State:%s=%d>' % (self.fingerprint, self.state)


class QueueModel(Model):
    __table_name__ = 'queue'

    partition_id = columns.Integer(primary_key=True)
    score = columns.Float(primary_key=True)
    created_at = columns.BigInt(primary_key=True)
    id = columns.UUID(primary_key=True, default=uuid.uuid4)
    url = columns.Text()
    fingerprint = columns.Text()
    host_crc32 = columns.Integer()
    meta = columns.Map(columns.Text(), columns.Text(), required=False)
    headers = columns.Map(columns.Text(), columns.Text(), required=False)
    cookies = columns.Map(columns.Text(), columns.Text(), required=False)
    method = columns.Text(required=False)
    depth = columns.SmallInt(required=False)

    @classmethod
    def query(cls, session):
        return session.query(cls)

    def __repr__(self):
        return '<Queue:%s (%d)>' % (self.url, self.id)
