# -*- coding: utf-8 -*-
import logging
from datetime import datetime
from time import time, sleep
import json

from cachetools import LRUCache
from frontera.contrib.backends.partitioners import Crc32NamePartitioner
from frontera.contrib.backends.memory import MemoryStates
from frontera.core.components import Metadata as BaseMetadata, Queue as BaseQueue
from frontera.core.models import Request, Response
from frontera.utils.misc import get_crc32
from frontera.utils.url import parse_domain_from_url_fast


def retry_and_rollback(func):
    def func_wrapper(self, *args, **kwargs):
        tries = 5
        while True:
            try:
                return func(self, *args, **kwargs)
            except Exception, exc:
                self.logger.exception(exc)
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
        self.session = session_cls
        self.model = model_cls
        self.table = 'MetadataModel'
        self.cache = LRUCache(cache_size)
        self.logger = logging.getLogger("frontera.contrib.backends.cassandra.components.Metadata")

    def frontier_stop(self):
        pass

    @retry_and_rollback
    def add_seeds(self, seeds):
        for seed in seeds:
            o = self._create_page(seed)
            self.cache[o.fingerprint] = o
            o.save()

    @retry_and_rollback
    def request_error(self, page, error):
        m = self._modify_page(page) if page.meta['fingerprint'] in self.cache else self._create_page(page)
        m.error = error
        self.cache[m.fingerprint] = m
        m.update()

    @retry_and_rollback
    def page_crawled(self, response, links):
        r = self._modify_page(response)
        self.cache[r.fingerprint] = r
        r.save()
        for link in links:
            if link.meta['fingerprint'] not in self.cache:
                l = self._create_page(link)
                self.cache[link.meta['fingerprint']] = l
                l.save()

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
        new_dict = {}
        for kmeta, vmeta in obj.meta.iteritems():
            if type(vmeta) is dict:
                new_dict[kmeta] = json.dumps(vmeta)
            else:
                new_dict[kmeta] = str(vmeta)

        db_page.meta = new_dict
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
            m.update()


class States(MemoryStates):

    def __init__(self, session_cls, model_cls, cache_size_limit):
        super(States, self).__init__(cache_size_limit)
        self.session = session_cls
        self.model = model_cls
        self.table = 'StateModel'
        self.logger = logging.getLogger("frontera.contrib.backends.cassandra.components.States")

    @retry_and_rollback
    def frontier_stop(self):
        pass

    @retry_and_rollback
    def fetch(self, fingerprints):
        to_fetch = [f for f in fingerprints if f not in self._cache]
        self.logger.debug("cache size %s" % len(self._cache))
        self.logger.debug("to fetch %d from %d" % (len(to_fetch), len(fingerprints)))

        for chunk in to_fetch:
            for state in self.model.objects.filter(fingerprint=chunk):
                self._cache[state.fingerprint] = state.state

    @retry_and_rollback
    def flush(self, force_clear=False):
        for fingerprint, state_val in self._cache.iteritems():
            state = self.model(fingerprint=fingerprint, state=state_val)
            state.save()
        self.logger.debug("State cache has been flushed.")
        super(States, self).flush(force_clear)


class Queue(BaseQueue):
    def __init__(self, session_cls, queue_cls, partitions, ordering='default'):
        self.session = session_cls
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
        if self.ordering == 'created_desc':
            return "-created_at"
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
            for item in self.queue_model.objects.filter(partition_id=partition_id).order_by("score", self._order_by()).\
                    limit(max_n_requests):
                item.meta = self.gen_meta(item.meta)
                method = 'GET' if not item.method else item.method
                r = Request(item.url, method=method, meta=item.meta, headers=item.headers, cookies=item.cookies)
                r.meta['fingerprint'] = item.fingerprint
                r.meta['score'] = item.score
                self.queue_model.delete(item)
                results.append(r)
                item.delete()

        except Exception, exc:
            self.logger.exception(exc)

        return results

    @retry_and_rollback
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
                created_at = time()*1E+6
                q = self._create_queue(request, fprint, score, partition_id, host_crc32, created_at)

                q.save()
                request.meta['state'] = States.QUEUED

    def _create_queue(self, obj, fingerprint, score, partition_id, host_crc32, created_at):
        db_queue = self.queue_model()
        db_queue.fingerprint = fingerprint
        db_queue.score = score
        db_queue.partition_id = partition_id
        db_queue.host_crc32 = host_crc32
        db_queue.url = obj.url
        db_queue.created_at = created_at

        new_dict = {}
        for kmeta, vmeta in obj.meta.iteritems():
            if type(vmeta) is dict:
                new_dict[kmeta] = json.dumps(vmeta)
            else:
                new_dict[kmeta] = str(vmeta)

        db_queue.meta = new_dict
        db_queue.depth = 0

        db_queue.headers = obj.headers
        db_queue.method = obj.method
        db_queue.cookies = obj.cookies

        return db_queue

    @retry_and_rollback
    def count(self):
        return self.queue_model.objects.count()

    def gen_meta(self, meta):
        ret_meta = {}
        for kmeta, vmeta in meta.iteritems():
            try:
                json_object = json.loads(vmeta)
            except ValueError, e:
                ret_meta[kmeta] = vmeta
            else:
                ret_meta[kmeta] = json_object

        if ret_meta['scrapy_callback'] == "None":
            ret_meta['scrapy_callback'] = None
        if ret_meta['scrapy_errback'] == "None":
            ret_meta['scrapy_errback'] = None
        if ret_meta['state'] == "None":
            ret_meta['state'] = None
        if ret_meta['origin_is_frontier'] == "True":
            ret_meta['origin_is_frontier'] = True
        ret_meta['depth'] = int(ret_meta['depth'])
        return ret_meta


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
            self.logger.debug("Try %d, limit %d, last attempt: requests %d, hosts %d" %
                              (tries, limit, count, len(queue.keys())))
            queue.clear()
            count = 0
            for item in self.queue_model.objects.filter(partition_id=partition_id).order_by(self._order_by()).\
                    limit(limit):
                item.meta = self.gen_meta(item.meta)
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
