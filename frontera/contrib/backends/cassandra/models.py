# -*- coding: utf-8 -*-
import uuid
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.usertype import UserType
from cassandra.cqlengine.columns import *


class Meta(UserType):
    domain = Map(Text(), Text(), required=False)
    fingerprint = Text()
    origin_is_frontier = Boolean()
    scrapy_callback = Text()
    scrapy_errback = Text()
    scrapy_meta = Map(Text(), Text(), required=False)
    score = Float(required=False)
    jid = Integer(required=False)

class MetadataModel(Model):
    __table_name__ = 'metadata'

    crawl = Text(primary_key=True)
    fingerprint = Text(primary_key=True)
    url = Text(index=True)
    depth = Integer()
    created_at = DateTime()
    fetched_at = DateTime(required=False)
    status_code = Integer(required=False)
    score = Float(required=False)
    error = Text(required=False)
    meta = UserDefinedType(Meta)
    headers = Map(Text(), Text(), required=False)
    cookies = Map(Text(), Text(), required=False)
    method = Text(required=False)

    @classmethod
    def query(cls, session):
        return session.query(cls)

    def __repr__(self):
        return '<Metadata:%s (%s)>' % (self.url, self.fingerprint)


class StateModel(Model):
    __table_name__ = 'states'

    crawl = Text(primary_key=True)
    fingerprint = Text(primary_key=True)
    state = SmallInt(index=True)

    @classmethod
    def query(cls, session):
        return session.query(cls)

    def __repr__(self):
        return '<State:%s=%d>' % (self.fingerprint, self.state)


class QueueModel(Model):
    __table_name__ = 'queue'

    crawl = Text(primary_key=True)
    partition_id = Integer(primary_key=True)
    score = Float(primary_key=True)
    created_at = BigInt(primary_key=True)
    fingerprint = Text(primary_key=True)
    url = Text()
    host_crc32 = Integer()
    meta = UserDefinedType(Meta)
    headers = Map(Text(), Text(), required=False)
    cookies = Map(Text(), Text(), required=False)
    method = Text(required=False)
    depth = SmallInt(required=False)

    @classmethod
    def query(cls, session):
        return session.query(cls)

    def __repr__(self):
        return '<Queue:%s (%d)>' % (self.url, self.id)


class CrawlStatsModel(Model):
    __table_name__ = 'crawlstats'

    crawl = Text(primary_key=True)
    pages_crawled = Counter()
    links_found = Counter()
    errors = Counter()
    seed_urls = Counter()
    scored_urls = Counter()
    queued_urls = Counter()
    dequeued_urls = Counter()

    @classmethod
    def query(cls, session):
        return session.query(cls)

    def __repr__(self):
        return '<CrawlStats:%s (%d)>' % (self.url, self.id)
