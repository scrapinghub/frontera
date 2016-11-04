# -*- coding: utf-8 -*-
import logging
import six
import sys
import traceback
import uuid
from time import time

from cachetools import LRUCache
from cassandra import (OperationTimedOut, ReadFailure, ReadTimeout,
                       WriteFailure, WriteTimeout)
from cassandra.concurrent import execute_concurrent_with_args
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


def _retry(func):
    def func_wrapper(self, *args, **kwargs):
        tries = 5
        count = 0
        while count < tries:
            try:
                return func(self, *args, **kwargs)
            except (OperationTimedOut, ReadTimeout, ReadFailure, WriteTimeout, WriteFailure) as exc:
                ex_type, ex, tb = sys.exc_info()
                tries += 1
                self.logger.warn("{0}: {1} Backtrace: {2}".format(ex_type.__name__, ex, traceback.extract_tb(tb)))
                del tb
                self.logger.info("Tries left %i" % tries - count)

        raise exc

    return func_wrapper


class Metadata(BaseMetadata, CreateOrModifyPageMixin):

    def __init__(self, session, model_cls, cache_size):
        self.session = session
        self.model = model_cls
        self.cache = LRUCache(cache_size)
        self.batch = BatchQuery()
        self.logger = logging.getLogger("frontera.contrib.backends.cassandra.components.Metadata")

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
        for fprint, (score, url, schedule) in six.iteritems(batch):
            page = self.cache[fprint]
            page.fingerprint = to_native_str(fprint)
            page.score = score
            self._add_to_batch_and_update_cache(page)
        self.batch.execute()

    def _add_to_batch_and_update_cache(self, page):
        self.cache[to_bytes(page.fingerprint)] = page.batch(self.batch).save()


class States(MemoryStates):

    def __init__(self, session, model_cls, cache_size_limit):
        super(States, self).__init__(cache_size_limit)
        self.session = session
        self.model = model_cls
        self.batch = BatchQuery()
        self.logger = logging.getLogger("frontera.contrib.backends.cassandra.components.States")

    def frontier_stop(self):
        self.flush()

    def fetch(self, fingerprints):
        to_fetch = [to_native_str(f) for f in fingerprints if f not in self._cache]
        self.logger.debug("cache size %s", len(self._cache))
        self.logger.debug("to fetch %d from %d", (len(to_fetch), len(fingerprints)))

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
    def __init__(self, session, queue_cls, partitions, crawl_id, generate_stats, ordering='default'):
        self.session = session
        self.queue_model = queue_cls
        self.logger = logging.getLogger("frontera.contrib.backends.cassandra.components.Queue")
        self.partitions = [i for i in range(0, partitions)]
        self.partitioner = Crc32NamePartitioner(self.partitions)
        self.ordering = ordering

    def frontier_stop(self):
        pass

    def _order_by(self):
        if self.ordering == 'created':
            return "created_at"
        return "created_at"

    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        """
        Dequeues new batch of requests for crawling.

        :param max_n_requests: maximum number of requests to return
        :param partition_id: partition id
        :return: list of :class:`Request <frontera.core.models.Request>` objects.
        """
        results = []
        try:
            dequeued_urls = 0
            cql_ditems = []
            d_query = self.session.prepare("DELETE FROM queue WHERE crawl = ? AND fingerprint = ? AND partition_id = ? "
                                           "AND score = ? AND created_at = ?")
            for item in self.queue_model.objects.filter(crawl=self.crawl_id, partition_id=partition_id).\
                    order_by("partition_id", "score", self._order_by()).limit(max_n_requests):
                method = 'GET' if not item.method else item.method

                meta_dict2 = dict((name, getattr(item.meta, name)) for name in dir(item.meta)
                                  if not name.startswith('__'))
                # TODO: How the result can be an dict not an object -> Objects get error while encodeing for Message Bus
                # If I take meta_dict2 direct to Request i get the same error message

                meta_dict = dict()
                meta_dict["fingerprint"] = meta_dict2["fingerprint"]
                meta_dict["domain"] = meta_dict2["domain"]
                meta_dict["origin_is_frontier"] = meta_dict2["origin_is_frontier"]
                meta_dict["scrapy_callback"] = meta_dict2["scrapy_callback"]
                meta_dict["scrapy_errback"] = meta_dict2["scrapy_errback"]
                meta_dict["scrapy_meta"] = meta_dict2["scrapy_meta"]
                meta_dict["score"] = meta_dict2["score"]
                meta_dict["jid"] = meta_dict2["jid"]

                r = Request(item.url, method=method, meta=meta_dict, headers=item.headers, cookies=item.cookies)
                r.meta['fingerprint'] = item.fingerprint
                r.meta['score'] = item.score
                results.append(r)

                cql_d = (item.crawl, item.fingerprint, item.partition_id, item.score, item.created_at)
                cql_ditems.append(cql_d)
                dequeued_urls += 1

            if dequeued_urls > 0:
                execute_concurrent_with_args(self.session, d_query, cql_ditems, concurrency=200)

            self.counter_cls.cass_count({"dequeued_urls": dequeued_urls})

        except Exception as exc:
            self.logger.exception(exc)

        return results

    def schedule(self, batch):
        query = self.session.prepare("INSERT INTO queue (id, fingerprint, score, partition_id, host_crc32, url, "
                                     "created_at, meta, depth, headers, method, cookies) "
                                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        cql_items = []
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
                created_at = time()*1E+6

                if "domain" not in request.meta:
                    request.meta["domain"] = {}
                if "origin_is_frontier" not in request.meta:
                    request.meta["origin_is_frontier"] = ''
                if "scrapy_callback" not in request.meta:
                    request.meta["scrapy_callback"] = None
                if "scrapy_errback" not in request.meta:
                    request.meta["scrapy_errback"] = None
                if "scrapy_meta" not in request.meta:
                    request.meta["scrapy_meta"] = {}
                if "score" not in request.meta:
                    request.meta["score"] = 0
                if "jid" not in request.meta:
                    request.meta["jid"] = 0

                cql_i = (uuid.uuid4(), fprint, score, partition_id, host_crc32, request.url, created_at,
                         request.meta, 0, request.headers, request.method, request.cookies)
                cql_items.append(cql_i)

                request.meta['state'] = States.QUEUED

        execute_concurrent_with_args(self.session, query, cql_items, concurrency=400)
        self.counter_cls.cass_count({"queued_urls": len(cql_items)})

    def count(self):
        count = self.queue_model.objects.filter().count()
        return count


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
            self.logger.debug("Try %d, limit %d, last attempt: requests %d, hosts %d" %
                              (tries, limit, count, len(queue.keys())))
            queue.clear()
            count = 0
            for item in self.queue_model.objects.filter(crawl=self.crawl_id, partition_id=partition_id).\
                    order_by("crawl", "score", self._order_by()).limit(limit):
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
                results.append(Request(item.url, method=method, meta=item.meta, headers=item.headers,
                                       cookies=item.cookies))
                item.delete()
        return results
