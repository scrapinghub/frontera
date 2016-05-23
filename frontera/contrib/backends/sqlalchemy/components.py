# -*- coding: utf-8 -*-
import logging
from datetime import datetime
from time import time, sleep

from cachetools import LRUCache
from frontera.contrib.backends.partitioners import Crc32NamePartitioner
from frontera.contrib.backends.memory import MemoryStates
from frontera.contrib.backends.sqlalchemy.models import DeclarativeBase
from frontera.core.components import Metadata as BaseMetadata, Queue as BaseQueue
from frontera.core.models import Request, Response
from frontera.utils.misc import get_crc32, chunks
from frontera.utils.url import parse_domain_from_url_fast


def retry_and_rollback(func):
    def func_wrapper(self, *args, **kwargs):
        tries = 5
        while True:
            try:
                return func(self, *args, **kwargs)
            except Exception, exc:
                self.logger.exception(exc)
                self.session.rollback()
                sleep(5)
                tries -= 1
                if tries > 0:
                    self.logger.info("Tries left %i" % tries)
                    continue
                else:
                    raise exc
    return func_wrapper


class Metadata(BaseMetadata):
    def __init__(self, session_cls, model_cls, cache_size):
        self.session = session_cls(expire_on_commit=False)   # FIXME: Should be explicitly mentioned in docs
        self.model = model_cls
        self.table = DeclarativeBase.metadata.tables['metadata']
        self.cache = LRUCache(cache_size)
        self.logger = logging.getLogger("sqlalchemy.metadata")

    def frontier_stop(self):
        self.session.close()

    @retry_and_rollback
    def add_seeds(self, seeds):
        for seed in seeds:
            o = self._create_page(seed)
            self.cache[o.fingerprint] = self.session.merge(o)
        self.session.commit()

    @retry_and_rollback
    def request_error(self, page, error):
        m = self._modify_page(page) if page.meta['fingerprint'] in self.cache else self._create_page(page)
        m.error = error
        self.cache[m.fingerprint] = self.session.merge(m)
        self.session.commit()

    @retry_and_rollback
    def page_crawled(self, response, links):
        r = self._modify_page(response) if response.meta['fingerprint'] in self.cache else self._create_page(response)
        self.cache[r.fingerprint] = self.session.merge(r)
        for link in links:
            if link.meta['fingerprint'] not in self.cache:
                self.cache[link.meta['fingerprint']] = self.session.merge(self._create_page(link))
        self.session.commit()

    def _modify_page(self, obj):
        db_page = self.cache[obj.meta['fingerprint']]
        db_page.fetched_at = datetime.utcnow()
        if isinstance(obj, Response):
            db_page.headers = obj.request.headers
            db_page.method = obj.request.method
            db_page.cookies = obj.request.cookies
            db_page.status_code = obj.status_code
        return db_page

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

    @retry_and_rollback
    def update_score(self, batch):
        for fprint, score, request, schedule in batch:
            m = self.model(fingerprint=fprint, score=score)
            self.session.merge(m)
        self.session.commit()


class States(MemoryStates):

    def __init__(self, session_cls, model_cls, cache_size_limit):
        super(States, self).__init__(cache_size_limit)
        self.session = session_cls()
        self.model = model_cls
        self.table = DeclarativeBase.metadata.tables['states']
        self.logger = logging.getLogger("sqlalchemy.states")

    @retry_and_rollback
    def frontier_stop(self):
        self.flush()
        self.session.close()

    @retry_and_rollback
    def fetch(self, fingerprints):
        to_fetch = [f for f in fingerprints if f not in self._cache]
        self.logger.debug("cache size %s", len(self._cache))
        self.logger.debug("to fetch %d from %d", len(to_fetch), len(fingerprints))

        for chunk in chunks(to_fetch, 128):
            for state in self.session.query(self.model).filter(self.model.fingerprint.in_(chunk)):
                self._cache[str(state.fingerprint)] = state.state

    @retry_and_rollback
    def flush(self, force_clear=False):
        for fingerprint, state_val in self._cache.iteritems():
            state = self.model(fingerprint=fingerprint, state=state_val)
            self.session.merge(state)
        self.session.commit()
        self.logger.debug("State cache has been flushed.")
        super(States, self).flush(force_clear)


class Queue(BaseQueue):
    def __init__(self, session_cls, queue_cls, partitions, ordering='default'):
        self.session = session_cls()
        self.queue_model = queue_cls
        self.logger = logging.getLogger("sqlalchemy.queue")
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
        return query.order_by(self.queue_model.score, self.queue_model.created_at)  # TODO: remove second parameter,
        # it's not necessary for proper crawling, but needed for tests

    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        """
        Dequeues new batch of requests for crawling.

        :param max_n_requests: maximum number of requests to return
        :param partition_id: partition id
        :return: list of :class:`Request <frontera.core.models.Request>` objects.
        """
        results = []
        try:
            for item in self._order_by(self.session.query(self.queue_model).filter_by(partition_id=partition_id)).\
                    limit(max_n_requests):
                method = 'GET' if not item.method else str(item.method)
                r = Request(item.url, method=method, meta=item.meta, headers=item.headers, cookies=item.cookies)
                r.meta['fingerprint'] = str(item.fingerprint)
                r.meta['score'] = item.score
                results.append(r)
                self.session.delete(item)
            self.session.commit()
        except Exception, exc:
            self.logger.exception(exc)
            self.session.rollback()
        return results

    @retry_and_rollback
    def schedule(self, batch):
        to_save = []
        for fprint, score, request, schedule in batch:
            if schedule:
                _, hostname, _, _, _, _ = parse_domain_from_url_fast(request.url)
                if not hostname:
                    self.logger.error("Can't get hostname for URL %s, fingerprint %s" % (request.url, fprint))
                    partition_id = self.partitions[0]
                    host_crc32 = 0
                else:
                    partition_id = self.partitioner.partition(hostname, self.partitions)
                    host_crc32 = get_crc32(hostname)
                q = self.queue_model(fingerprint=fprint, score=score, url=request.url, meta=request.meta,
                                     headers=request.headers, cookies=request.cookies, method=request.method,
                                     partition_id=partition_id, host_crc32=host_crc32, created_at=time()*1E+6)
                to_save.append(q)
                request.meta['state'] = States.QUEUED
        self.session.bulk_save_objects(to_save)
        self.session.commit()

    @retry_and_rollback
    def count(self):
        return self.session.query(self.queue_model).count()


class BroadCrawlingQueue(Queue):

    GET_RETRIES = 3

    @retry_and_rollback
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
            self.logger.debug("Try %d, limit %d, last attempt: requests %d, hosts %d",
                              tries, limit, count, len(queue.keys()))
            queue.clear()
            count = 0
            for item in self._order_by(self.session.query(self.queue_model).filter_by(partition_id=partition_id)).\
                    limit(limit):
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
        self.logger.debug("Finished: tries %d, hosts %d, requests %d", tries, len(queue.keys()), count)

        results = []
        for items in queue.itervalues():
            for item in items:
                method = 'GET' if not item.method else str(item.method)
                results.append(Request(item.url, method=method,
                                       meta=item.meta, headers=item.headers, cookies=item.cookies))
                self.session.delete(item)
        self.session.commit()
        return results