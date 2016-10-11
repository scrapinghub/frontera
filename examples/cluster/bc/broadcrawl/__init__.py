# -*- coding: utf-8 -*-
from frontera.core.components import States
from frontera.worker.strategies import BaseCrawlingStrategy
from frontera.contrib.backends.hbase import HBaseBackend
from cachetools import LRUCache
from msgpack import packb, unpackb
import logging
from datetime import timedelta

from six.moves.urllib import parse as urlparse
import six


class DomainCache(LRUCache):
    def __init__(self, maxsize, connection, table_name):
        super(DomainCache, self).__init__(maxsize)
        self.table = connection.table(table_name)

    def popitem(self):
        key, value = super(DomainCache, self).popitem()
        self._store_item(self.table, key, value)

    def __missing__(self, key):
        row = self.table.row(key)
        if not row:
            super(DomainCache, self).__missing__(key)
            raise KeyError
        value = {}
        for k, v in row.iteritems():
            cf, _, col = k.partition(':')
            value[col] = unpackb(v)
        self.__setitem__(key, value)
        return value

    def _store_item(self, batch, key, value):
        data = {}
        assert isinstance(value, dict)
        for k, v in six.iteritems(value):
            data["m:%s" % k] = packb(v)
        batch.put(key, data)

    def flush(self):
        with self.table.batch() as b:
            for k, v in six.iteritems(self):
                self._store_item(b, k, v)


class BCPerHostLimit(BaseCrawlingStrategy):

    def __init__(self, manager, mb_stream, states_context):
        settings = manager.settings
        backend = manager.backend
        assert isinstance(backend, HBaseBackend), "This strategy supports HBaseBackend only."
        self.conn = backend.connection
        self.domain_cache = DomainCache(10000, self.conn, "domain_metadata")
        self.max_pages_per_hostname = settings.get("MAX_PAGES_PER_HOSTNAME")
        assert self.max_pages_per_hostname is not None
        self.logger = logging.getLogger("bcperhostlimit-strategy")
        super(BCPerHostLimit, self).__init__(manager, mb_stream, states_context)

    @classmethod
    def from_worker(cls, manager, mb_scheduler, states_context):
        return cls(manager, mb_scheduler, states_context)

    def add_seeds(self, seeds):
        self._schedule_and_count(seeds)

    def page_crawled(self, response):
        response.meta[b'state'] = States.CRAWLED
        domain = self._get_domain_bucket(response.url)
        domain['cp'] = domain.get('cp', 0)+1

    def links_extracted(self, request, links):
        self._schedule_and_count(links)

    def page_error(self, request, error):
        request.meta['state'] = States.ERROR
        self.schedule(request, score=0.0, dont_queue=True)

    def _schedule_and_count(self, links):
        counts = dict()
        for link in links:
            if link.meta[b'state'] is not States.NOT_CRAWLED:
                continue
            link.meta[b'state'] = States.QUEUED
            url_parts = urlparse.urlparse(link.url)
            if not url_parts.hostname:
                continue
            hostname = url_parts.hostname
            if hostname not in counts:
                domain = self.domain_cache.setdefault(hostname, {})
                counts[hostname] = domain.get('sc', 0)
            if counts[hostname] >= self.max_pages_per_hostname:
                self.logger.debug("Reached per host limit for URL %s, "
                                 "already scheduled %d of %d allowed.", link.url, counts[hostname],
                                 self.max_pages_per_hostname)
                continue
            path_parts = url_parts.path.split('/')
            score = 0.5 / (max(len(path_parts), 1.0) + len(url_parts.path) * 0.1)
            self.schedule(link, score)
            counts[hostname] += 1
            if counts[hostname] == self.max_pages_per_hostname:
                self.logger.info("Reached per host limit for domain %s (%d)", hostname, self.max_pages_per_hostname)

        for hostname, count in six.iteritems(counts):
            domain = self.domain_cache.setdefault(hostname, {})
            domain['sc'] = domain.get('sc', 0)+count

    def _get_domain_bucket(self, url):
        parsed = urlparse.urlsplit(url)
        hostname, _, _ = parsed.netloc.partition(':')
        return self.domain_cache.setdefault(hostname, {})

    def close(self):
        self.domain_cache.flush()
        super(BCPerHostLimit, self).close()
