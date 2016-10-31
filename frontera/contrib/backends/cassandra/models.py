# -*- coding: utf-8 -*-
from cassandra.cqlengine.columns import (UUID, BigInt, DateTime, Float,
                                         Integer, Map, SmallInt, Text)
from cassandra.cqlengine.models import Model


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
    meta = Map(Text(), Text())
    headers = Map(Text(), Text())
    cookies = Map(Text(), Text())
    method = Text()

    def __repr__(self):
        return '<Metadata:%s (%s)>' % (self.url, self.fingerprint)


class StateModel(Model):
    __table_name__ = 'states'

    fingerprint = Text(primary_key=True)
    state = SmallInt(required=True)

    def __repr__(self):
        return '<State:%s=%d>' % (self.fingerprint, self.state)


class QueueModel(Model):
    __table_name__ = 'queue'

    id = UUID(primary_key=True)
    partition_id = Integer(required=True)
    score = Float(required=True)
    url = Text(required=True)
    fingerprint = Text(required=True)
    host_crc32 = Integer(required=True)
    meta = Map(Text(), Text())
    headers = Map(Text(), Text())
    cookies = Map(Text(), Text())
    method = Text()
    created_at = BigInt(required=True)
    depth = SmallInt()

    def __repr__(self):
        return '<Queue:%s (%d)>' % (self.url, self.id)
