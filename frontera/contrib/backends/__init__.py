# -*- coding: utf-8 -*-
from __future__ import absolute_import
from collections import OrderedDict

from frontera import Backend
from frontera.core.components import States


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
