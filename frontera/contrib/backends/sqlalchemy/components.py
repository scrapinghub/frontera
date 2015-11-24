# -*- coding: utf-8 -*-
import logging
from datetime import datetime
from frontera.core.components import Metadata, States, Queue
from frontera.core.models import Request, Response
from frontera.utils.url import parse_domain_from_url_fast
from frontera.contrib.backends import Crc32NamePartitioner
from frontera.utils.misc import get_crc32, chunks
from frontera.contrib.backends.sqlalchemy.models import DeclarativeBase


class SQLAlchemyMetadata(Metadata):
    def __init__(self, session_cls, model_cls):
        self.session = session_cls(expire_on_commit=False)   # FIXME: Should be explicitly mentioned in docs
        self.model = model_cls
        self.table = DeclarativeBase.metadata.tables['metadata']

    def frontier_stop(self):
        self.session.close()

    def add_seeds(self, seeds):
        self.session.add_all(map(self._create_page, seeds))
        self.session.commit()

    def request_error(self, page, error):
        m = self._create_page(page)
        m.error = error
        self.session.merge(m)
        self.session.commit()

    def page_crawled(self, response, links):
        self.session.merge(self._create_page(response))
        for link in links:
            self.session.merge(self._create_page(link))
        self.session.commit()

    def _create_page(self, obj):
        db_page = self.model()
        db_page.fingerprint = obj.meta['fingerprint']
        db_page.url = obj.url
        db_page.created_at = datetime.utcnow()
        db_page.meta = obj.meta
        db_page.depth = 0

        if isinstance(obj, Request):
            db_page.headers = obj.headers
            db_page.method = obj.method
            db_page.cookies = obj.cookies
        elif isinstance(obj, Response):
            db_page.headers = obj.request.headers
            db_page.method = obj.request.method
            db_page.cookies = obj.request.cookies
            db_page.status_code = obj.status_code
        return db_page


class SQLAlchemyState(States):
    NOT_CRAWLED = 0
    QUEUED = 1
    CRAWLED = 2
    ERROR = 3

    def __init__(self, session_cls, model_cls, cache_size_limit):
        self.session = session_cls()
        self.model = model_cls
        self.table = DeclarativeBase.metadata.tables['states']
        self._cache = dict()
        self.logger = logging.getLogger("frontera.contrib.backends.sqlalchemy.SQLAlchemyState")
        self._cache_size_limit = cache_size_limit

    def frontier_stop(self):
        self.flush()
        self.session.close()

    def fetch(self, fingerprints):
        to_fetch = [f for f in fingerprints if f not in self._cache]
        self.logger.debug("cache size %s" % len(self._cache))
        self.logger.debug("to fetch %d from %d" % (len(to_fetch), len(fingerprints)))

        for chunk in chunks(to_fetch, 128):
            for state in self.session.query(self.model).filter(self.model.fingerprint.in_(chunk)):
                self._cache[state.fingerprint] = state.state

    def flush(self, force_clear=False):
        if len(self._cache) > self._cache_size_limit:
            force_clear = True
        for fingerprint, state_val in self._cache.iteritems():
            state = self.model(fingerprint=fingerprint, state=state_val)
            self.session.add(state)
        self.session.commit()
        self.logger.debug("State cache has been flushed.")
        if force_clear:
            self.logger.debug("Cache has %d items, clearing" % len(self._cache))
            self._cache.clear()

    def _put(self, obj):
        if obj.meta['state'] is not None:
            self._cache[obj.meta['fingerprint']] = obj.meta['state']

    def _get(self, obj):
        fprint = obj.meta['fingerprint']
        obj.meta['state'] = self._cache[fprint] if fprint in self._cache else None

    def update_cache(self, objs):
        objs = objs if type(objs) in [list, tuple] else [objs]
        map(self._put, objs)

    def set_states(self, objs):
        objs = objs if type(objs) in [list, tuple] else [objs]
        map(self._get, objs)


class SQLAlchemyQueue(Queue):

    GET_RETRIES = 3

    def __init__(self, session_cls, queue_cls, metadata_cls, partitions, ordering='default'):
        self.session = session_cls()
        self.queue_model = queue_cls
        self.metadata_model = metadata_cls
        self.table = DeclarativeBase.metadata.tables['queue']
        self.logger = logging.getLogger("frontera.contrib.backends.sqlalchemy.SQLAlchemyQueue")
        self.partitions = [i for i in range(0, partitions)]
        self.partitioner = Crc32NamePartitioner(self.partitions)
        self.ordering = ordering

    def frontier_stop(self):
        self.session.close()

    def _order_by(self, query):
        if self.ordering == 'created':
            return query.order_by(self.queue_model.created_at)
        if self.ordering == 'created_desc':
            return query.order_by(self.queue_model.created_at.desc())
        return query.order_by(self.queue_model.score)

    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        """
        Dequeues new batch of requests for crawling.

        Priorities, from highest to lowest:
         - max_requests_per_host
         - max_n_requests
         - min_hosts & min_requests

        :param max_n_requests:
        :param partition_id:
        :param kwargs: min_requests, min_hosts, max_requests_per_host
        :return: list of :class:`Request <frontera.core.models.Request>` objects.
        """
        min_requests = kwargs.pop("min_requests", None)
        min_hosts = kwargs.pop("min_hosts", None)
        max_requests_per_host = kwargs.pop("max_requests_per_host", None)
        assert(max_n_requests > min_requests)

        queue = {}
        limit = max_n_requests
        tries = 0
        count = 0
        while tries < self.GET_RETRIES:
            tries += 1
            limit *= 5.5 if tries > 1 else 1.0
            self.logger.debug("Try %d, limit %d, last attempt: requests %d, hosts %d" %
                              (tries, limit, count, len(queue.keys())))
            queue.clear()
            count = 0
            for item in self._order_by(self.session.query(self.queue_model).filter_by(partition_id=partition_id)).limit(limit):
                if item.host_crc32 not in queue:
                    queue[item.host_crc32] = []
                if max_requests_per_host is not None and len(queue[item.host_crc32]) > max_requests_per_host:
                    continue
                queue[item.host_crc32].append(item)
                count += 1
                if count > max_n_requests:
                    break
            if min_hosts is not None and len(queue.keys()) < min_hosts:
                continue
            if min_requests is not None and count < min_requests:
                continue
            break
        self.logger.debug("Finished: tries %d, hosts %d, requests %d" % (tries, len(queue.keys()), count))

        results = []
        for items in queue.itervalues():
            for item in items:
                method = 'GET' if not item.method else item.method
                results.append(Request(item.url, method=method, meta=item.meta, headers=item.headers, cookies=item.cookies))
                self.session.delete(item)
        self.session.commit()
        return results

    def schedule(self, batch):
        for fprint, (score, request, schedule) in batch.iteritems():
            if schedule:
                _, hostname, _, _, _, _ = parse_domain_from_url_fast(request.url)
                if not hostname:
                    self.logger.error("Can't get hostname for URL %s, fingerprint %s" % (request.url, fprint))
                    continue
                partition_id = self.partitioner.partition(hostname, self.partitions)
                host_crc32 = get_crc32(hostname)
                q = self.queue_model(fingerprint=fprint, score=score, url=request.url, meta=request.meta,
                                     headers=request.headers, cookies=request.cookies, method=request.method,
                                     partition_id=partition_id, host_crc32=host_crc32, created_at=datetime.utcnow())
                self.session.add(q)
            m = self.metadata_model(fingerprint=fprint, score=score)
            self.session.merge(m)
        self.session.commit()

    def count(self):
        return self.session.query(self.queue_model).count()