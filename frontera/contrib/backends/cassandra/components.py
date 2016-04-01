# -*- coding: utf-8 -*-
import logging
from datetime import datetime
from time import time, sleep
from cachetools import LRUCache
from frontera.contrib.backends.partitioners import Crc32NamePartitioner
from frontera.contrib.backends.memory import MemoryStates
from frontera.core.components import Metadata as BaseMetadata, Queue as BaseQueue
from frontera.core.models import Request, Response
from frontera.utils.misc import get_crc32, chunks
from frontera.utils.url import parse_domain_from_url_fast
from cassandra.concurrent import execute_concurrent_with_args
from frontera.contrib.backends.cassandra.models import Meta


class Metadata(BaseMetadata):
    def __init__(self, session, model_cls, cache_size, crawl_id):
        self.session = session
        self.model = model_cls
        self.table = 'MetadataModel'
        self.cache = LRUCache(cache_size)
        self.logger = logging.getLogger("frontera.contrib.backends.cassandra.components.Metadata")
        self.crawl_id = crawl_id

    def frontier_stop(self):
        pass

    def add_seeds(self, seeds):
        cql_items = []
        for seed in seeds:
            o = self._create_page(seed)
            self.cache[o.fingerprint] = o

            query = self.session.prepare(
                "INSERT INTO metadata (crawl, fingerprint, url, created_at, meta, headers, cookies, method, depth) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
            meta = Meta(domain=seed.meta['domain'], fingerprint=seed.meta['fingerprint'],
                        origin_is_frontier=seed.meta['origin_is_frontier'],
                        scrapy_callback=seed.meta['scrapy_callback'], scrapy_errback=seed.meta['scrapy_errback'],
                        scrapy_meta=seed.meta['scrapy_meta'])
            cql_i = (self.crawl_id, seed.meta['fingerprint'], seed.url, datetime.utcnow(), meta,
                     seed.headers, seed.cookies, seed.method, o.depth)
            cql_items.append(cql_i)
        if len(seeds) > 0:
            execute_concurrent_with_args(self.session, query, cql_items, concurrency=400)
        self.cass_count({"seed_urls": len(seeds)})

    def request_error(self, page, error):
        m = self._create_page(page)
        m.error = error
        self.cache[m.fingerprint] = m
        query_page = self.session.prepare(
            "UPDATE metadata SET error = ? WHERE crawl = ? AND  fingerprint = ?")
        self.session.execute(query_page, (error, self.crawl_id, page.meta['fingerprint']))
        self.cass_count({"error": 1})

    def page_crawled(self, response, links):
        query_page = self.session.prepare(
            "UPDATE metadata SET fetched_at = ?, headers = ?, method = ?, cookies = ?, status_code = ? "
            "WHERE crawl= ? AND fingerprint = ?")
        self.session.execute_async(query_page, (datetime.utcnow(), response.request.headers, response.request.method,
                                                response.request.cookies, response.status_code, self.crawl_id,
                                                response.meta['fingerprint']))

        query = self.session.prepare(
            "INSERT INTO metadata (crawl, fingerprint, created_at, method, url, depth) VALUES (?, ?, ?, ?, ?, ?)")
        cql_items = []
        for link in links:
            if link.meta['fingerprint'] not in self.cache:
                link.depth = self.cache[response.meta['fingerprint']].depth+1
                l = self._create_page(link)
                self.cache[link.meta['fingerprint']] = l
                cql_i = (self.crawl_id, link.meta['fingerprint'], datetime.utcnow(), link.method, link.url, link.depth)
                cql_items.append(cql_i)
        execute_concurrent_with_args(self.session, query, cql_items, concurrency=400)
        self.cass_count({"pages_crawled": 1, "links_found": len(cql_items)})

    def _create_page(self, obj):
        db_page = self.model()
        db_page.fingerprint = obj.meta['fingerprint']
        db_page.url = obj.url
        db_page.created_at = datetime.utcnow()
        db_page.meta = obj.meta
        if hasattr(obj, 'depth'):
            db_page.depth = obj.depth
        else:
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

    def update_score(self, batch):
        query = self.session.prepare("UPDATE metadata SET score = ? WHERE crawl = ? AND fingerprint = ?")
        cql_items = []
        for fprint, score, request, schedule in batch:
            cql_i = (score, self.crawl_id, fprint)
            cql_items.append(cql_i)
        execute_concurrent_with_args(self.session, query, cql_items, concurrency=400)
        self.cass_count({"scored_urls": len(cql_items)})

    def cass_count(self, counts):
        for row, count in counts.iteritems():
            count_page = self.session.prepare("UPDATE crawlstats SET "+row+" = "+row+" + ? WHERE crawl= ?")
            self.session.execute_async(count_page, (count, self.crawl_id))


class States(MemoryStates):

    def __init__(self, session, model_cls, cache_size_limit, crawl_id):
        super(States, self).__init__(cache_size_limit)
        self.session = session
        self.model = model_cls
        self.table = 'StateModel'
        self.logger = logging.getLogger("frontera.contrib.backends.cassandra.components.States")
        self.crawl_id = crawl_id

    def frontier_stop(self):
        pass

    def fetch(self, fingerprints):
        to_fetch = [f for f in fingerprints if f not in self._cache]
        self.logger.debug("cache size %s", len(self._cache))
        self.logger.debug("to fetch %d from %d", (len(to_fetch), len(fingerprints)))

        for chunk in chunks(to_fetch, 128):
            for state in self.model.objects.filter(crawl=self.crawl_id, fingerprint__in=chunk):
                self._cache[state.fingerprint] = state.state

    def flush(self, force_clear=False):
        query = self.session.prepare("INSERT INTO states (crawl, fingerprint, state) VALUES (?, ?, ?)")
        cql_items = []
        for fingerprint, state_val in self._cache.iteritems():
            cql_i = (self.crawl_id, fingerprint, state_val)
            cql_items.append(cql_i)
        execute_concurrent_with_args(self.session, query, cql_items, concurrency=20000)
        super(States, self).flush(force_clear)


class Queue(BaseQueue):
    def __init__(self, session, queue_cls, partitions, crawl_id, ordering='default'):
        self.session = session
        self.queue_model = queue_cls
        self.logger = logging.getLogger("frontera.contrib.backends.cassandra.components.Queue")
        self.partitions = [i for i in range(0, partitions)]
        self.partitioner = Crc32NamePartitioner(self.partitions)
        self.ordering = ordering
        self.crawl_id = crawl_id

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

            self.cass_count({"dequeued_urls": dequeued_urls})

        except Exception, exc:
            self.logger.exception(exc)

        return results

    def schedule(self, batch):
        query = self.session.prepare("INSERT INTO queue (crawl, fingerprint, score, partition_id, host_crc32, url, "
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

                meta = Meta(domain=request.meta['domain'], fingerprint=fprint,
                            origin_is_frontier=request.meta['origin_is_frontier'],
                            scrapy_callback=request.meta['scrapy_callback'],
                            scrapy_errback=request.meta['scrapy_errback'], scrapy_meta=request.meta['scrapy_meta'])

                cql_i = (self.crawl_id, fprint, score, partition_id, host_crc32, request.url, created_at, meta, 0,
                         request.headers, request.method, request.cookies)
                cql_items.append(cql_i)

                request.meta['state'] = States.QUEUED

        execute_concurrent_with_args(self.session, query, cql_items, concurrency=400)
        self.cass_count({"queued_urls": len(cql_items)})

    def count(self):
        count = self.queue_model.objects.filter(crawl=self.crawl_id).count()
        return count

    def cass_count(self, counts):
        for row, count in counts.iteritems():
            count_page = self.session.prepare("UPDATE crawlstats SET " + row + " = " + row + " + ? WHERE crawl= ?")
            self.session.execute_async(count_page, (count, self.crawl_id))


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
