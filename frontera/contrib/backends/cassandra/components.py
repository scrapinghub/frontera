# -*- coding: utf-8 -*-
import logging
import uuid
from time import time

import six
from cachetools import LRUCache
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.query import BatchQuery
from w3lib.util import to_bytes, to_native_str

from frontera.contrib.backends import CreateOrModifyPageMixin
from frontera.contrib.backends.memory import MemoryStates
from frontera.contrib.backends.partitioners import Crc32NamePartitioner
from frontera.core.components import Metadata as BaseMetadata
from frontera.core.components import Queue as BaseQueue
from frontera.core.models import Request
from frontera.utils.misc import chunks, get_crc32
from frontera.utils.url import parse_domain_from_url_fast


class Metadata(BaseMetadata, CreateOrModifyPageMixin):

    def __init__(self, model_cls, cache_size):
        self.model = model_cls
        self.cache = LRUCache(cache_size)
        self.batch = BatchQuery()
        self.logger = logging.getLogger("frontera.contrib.backends.cassandra.components.Metadata")
        sync_table(model_cls)

    def frontier_stop(self):
        pass

    def add_seeds(self, seeds):
        for seed in seeds:
            page = self._create_page(seed)
            self._add_to_batch_and_update_cache(page)
        self.batch.execute()

    def request_error(self, page, error):
        page = self._modify_page(page) if page.meta[b'fingerprint'] in self.cache else self._create_page(page)
        page.error = error
        self._add_to_batch_and_update_cache(page)
        self.batch.execute()

    def page_crawled(self, response):
        page = self._modify_page(response) \
            if response.meta[b'fingerprint'] in self.cache else self._create_page(response)
        self._add_to_batch_and_update_cache(page)
        self.batch.execute()

    def links_extracted(self, request, links):
        for link in links:
            if link.meta[b'fingerprint'] not in self.cache:
                page = self._create_page(link)
                self._add_to_batch_and_update_cache(page)
        self.batch.execute()

    def update_score(self, batch):
        if isinstance(batch, dict):
            batch = [(fprint, score, url, schedule) for fprint, (score, url, schedule) in six.iteritems(batch)]
        for fprint, score, url, schedule in batch:
            page = self.cache[fprint]
            page.fingerprint = to_native_str(fprint)
            page.score = score
            self._add_to_batch_and_update_cache(page)
        self.batch.execute()

    def _add_to_batch_and_update_cache(self, page):
        self.cache[to_bytes(page.fingerprint)] = page.batch(self.batch).save()


class States(MemoryStates):

    def __init__(self, model_cls, cache_size_limit):
        super(States, self).__init__(cache_size_limit)
        self.model = model_cls
        self.batch = BatchQuery()
        self.logger = logging.getLogger("frontera.contrib.backends.cassandra.components.States")
        sync_table(model_cls)

    def frontier_stop(self):
        self.flush()

    def fetch(self, fingerprints):
        to_fetch = [to_native_str(f) for f in fingerprints if f not in self._cache]
        self.logger.debug("cache size %s", len(self._cache))
        self.logger.debug("to fetch %d from %d", len(to_fetch), len(fingerprints))

        for chunk in chunks(to_fetch, 128):
            for state in self.model.objects.filter(fingerprint__in=chunk):
                self._cache[to_bytes(state.fingerprint)] = state.state

    def flush(self, force_clear=False):
        for fingerprint, state_val in six.iteritems(self._cache):
            state = self.model(fingerprint=to_native_str(fingerprint), state=state_val)
            state.batch(self.batch).save()
        self.batch.execute()
        self.logger.debug("State cache has been flushed.")
        super(States, self).flush(force_clear)


class Queue(BaseQueue):

    def __init__(self, queue_cls, partitions, ordering='default'):
        self.queue_model = queue_cls
        self.logger = logging.getLogger("frontera.contrib.backends.cassandra.components.Queue")
        self.partitions = [i for i in range(0, partitions)]
        self.partitioner = Crc32NamePartitioner(self.partitions)
        self.ordering = ordering
        self.batch = BatchQuery()
        sync_table(queue_cls)

    def frontier_stop(self):
        pass

    def _order_by(self, query):
        if self.ordering == 'created':
            return query.order_by('created_at')
        if self.ordering == 'created_desc':
            return query.order_by('-created_at')
        return query.order_by('score', 'created_at')  # TODO: remove second parameter,
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
            for item in self._order_by(self.queue_model.filter(partition_id=partition_id).
                                               allow_filtering()).limit(max_n_requests):
                method = item.method or b'GET'
                r = Request(item.url, method=method, meta=item.meta, headers=item.headers, cookies=item.cookies)
                r.meta[b'fingerprint'] = to_bytes(item.fingerprint)
                r.meta[b'score'] = item.score
                results.append(r)
                item.batch(self.batch).delete()
            self.batch.execute()
        except Exception as exc:
            self.logger.exception(exc)
        return results

    def schedule(self, batch):
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
                q = self.queue_model(id=uuid.uuid4(),
                                     fingerprint=to_native_str(fprint),
                                     score=score,
                                     url=request.url,
                                     meta=request.meta,
                                     headers=request.headers,
                                     cookies=request.cookies,
                                     method=to_native_str(request.method),
                                     partition_id=partition_id,
                                     host_crc32=host_crc32,
                                     created_at=time() * 1E+6)
                q.batch(self.batch).save()
                request.meta[b'state'] = States.QUEUED
        self.batch.execute()

    def count(self):
        return self.queue_model.all().count()


class BroadCrawlingQueue(Queue):
    GET_RETRIES = 3

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
            for item in self._order_by(self.queue_model.filter(partition_id=partition_id).
                                               allow_filtering()).limit(max_n_requests):
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
        for items in six.itervalues(queue):
            for item in items:
                method = item.method or b'GET'
                results.append(Request(item.url,
                                       method=method,
                                       meta=item.meta,
                                       headers=item.headers,
                                       cookies=item.cookies))
                item.batch(self.batch).delete()
        self.batch.execute()
        return results
