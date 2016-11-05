# -*- coding: utf-8 -*-
from __future__ import absolute_import
from collections import OrderedDict
from datetime import datetime

from frontera import Backend
from frontera.core.components import States, Queue as BaseQueue, DistributedBackend
from frontera.core.models import Request, Response

from w3lib.util import to_native_str


class CommonBackend(Backend):
    """
    A simpliest possible backend, performing one-time crawl: if page was crawled once, it will not be crawled again.
    """
    component_name = 'Common Backend'

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def frontier_start(self):
        self.metadata.frontier_start()
        self.queue.frontier_start()
        self.states.frontier_start()
        self.queue_size = self.queue.count()

    def frontier_stop(self):
        self.metadata.frontier_stop()
        self.queue.frontier_stop()
        self.states.frontier_stop()

    def add_seeds(self, seeds):
        for seed in seeds:
            seed.meta[b'depth'] = 0
        self.metadata.add_seeds(seeds)
        self.states.fetch([seed.meta[b'fingerprint'] for seed in seeds])
        self.states.set_states(seeds)
        self._schedule(seeds)
        self.states.update_cache(seeds)

    def _schedule(self, requests):
        batch = []
        queue_incr = 0
        for request in requests:
            schedule = True if request.meta[b'state'] in [States.NOT_CRAWLED, States.ERROR, None] else False
            batch.append((request.meta[b'fingerprint'], self._get_score(request), request, schedule))
            if schedule:
                queue_incr += 1
                request.meta[b'state'] = States.QUEUED
        self.queue.schedule(batch)
        self.metadata.update_score(batch)
        self.queue_size += queue_incr

    def _get_score(self, obj):
        return obj.meta.get(b'score', 1.0)

    def get_next_requests(self, max_next_requests, **kwargs):
        partitions = kwargs.pop('partitions', [0])  # TODO: Collect from all known partitions
        batch = []
        for partition_id in partitions:
            batch.extend(self.queue.get_next_requests(max_next_requests, partition_id, **kwargs))
        self.queue_size -= len(batch)
        return batch

    def page_crawled(self, response):
        response.meta[b'state'] = States.CRAWLED
        self.states.update_cache(response)
        self.metadata.page_crawled(response)

    def links_extracted(self, request, links):
        to_fetch = OrderedDict()
        for link in links:
            to_fetch[link.meta[b'fingerprint']] = link
            link.meta[b'depth'] = request.meta.get(b'depth', 0)+1
        self.states.fetch(to_fetch.keys())
        self.states.set_states(links)
        unique_links = to_fetch.values()
        self.metadata.links_extracted(request, unique_links)
        self._schedule(unique_links)
        self.states.update_cache(unique_links)

    def request_error(self, request, error):
        request.meta[b'state'] = States.ERROR
        self.metadata.request_error(request, error)
        self.states.update_cache(request)

    def finished(self):
        return self.queue_size == 0


class CommonStorageBackend(CommonBackend):

    def _create_queue(self, settings):
        if not issubclass(self.queue_component, BaseQueue):
            raise TypeError('expected queue_component to '
                            'belong to class: %s, got %s instead' % (type(BaseQueue).__name__,
                                                                     type(self.queue_component).__name__))
        return self.queue_component(self.session,
                                    self.models['QueueModel'],
                                    settings.get('SPIDER_FEED_PARTITIONS'))

    @property
    def queue(self):
        return self._queue

    @property
    def metadata(self):
        return self._metadata

    @property
    def states(self):
        return self._states


class CommonDistributedStorageBackend(DistributedBackend):

    @property
    def queue(self):
        return self._queue

    @property
    def metadata(self):
        return self._metadata

    @property
    def states(self):
        return self._states

    def frontier_start(self):
        for component in [self.metadata, self.queue, self.states]:
            if component:
                component.frontier_start()

    def frontier_stop(self):
        for component in [self.metadata, self.queue, self.states]:
            if component:
                component.frontier_stop()

    def add_seeds(self, seeds):
        self.metadata.add_seeds(seeds)

    def get_next_requests(self, max_next_requests, **kwargs):
        partitions = kwargs.pop('partitions', [0])  # TODO: Collect from all known partitions
        batch = []
        for partition_id in partitions:
            batch.extend(self.queue.get_next_requests(max_next_requests, partition_id, **kwargs))
        return batch

    def page_crawled(self, response):
        self.metadata.page_crawled(response)

    def links_extracted(self, request, links):
        self.metadata.links_extracted(request, links)

    def request_error(self, request, error):
        self.metadata.request_error(request, error)

    def finished(self):
        raise NotImplementedError


class CreateOrModifyPageMixin(object):

    def _create_page(self, obj):
        db_page = self.model()
        db_page.fingerprint = to_native_str(obj.meta[b'fingerprint'])
        db_page.url = obj.url
        db_page.created_at = datetime.utcnow()
        db_page.meta = obj.meta
        db_page.depth = 0

        if isinstance(obj, Request):
            db_page.headers = obj.headers
            db_page.method = to_native_str(obj.method)
            db_page.cookies = obj.cookies
        elif isinstance(obj, Response):
            db_page.headers = obj.request.headers
            db_page.method = to_native_str(obj.request.method)
            db_page.cookies = obj.request.cookies
            db_page.status_code = obj.status_code
        return db_page

    def _modify_page(self, obj):
        db_page = self.cache[obj.meta[b'fingerprint']]
        db_page.fetched_at = datetime.utcnow()
        if isinstance(obj, Response):
            db_page.headers = obj.request.headers
            db_page.method = to_native_str(obj.request.method)
            db_page.cookies = obj.request.cookies
            db_page.status_code = obj.status_code
        return db_page
