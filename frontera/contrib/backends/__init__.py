# -*- coding: utf-8 -*-
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
            seed.meta['depth'] = 0
        self.metadata.add_seeds(seeds)
        self.states.fetch([seed.meta['fingerprint'] for seed in seeds])
        self.states.set_states(seeds)
        self._schedule(seeds)
        self.states.update_cache(seeds)

    def _schedule(self, requests):
        batch = []
        queue_incr = 0
        for request in requests:
            schedule = True if request.meta['state'] in [States.NOT_CRAWLED, States.ERROR, None] else False
            batch.append((request.meta['fingerprint'], self._get_score(request), request, schedule))
            if schedule:
                queue_incr += 1
                request.meta['state'] = States.QUEUED
        self.queue.schedule(batch)
        self.metadata.update_score(batch)
        self.queue_size += queue_incr

    def _get_score(self, obj):
        return obj.meta.get('score', 1.0)

    def get_next_requests(self, max_next_requests, **kwargs):
        partitions = kwargs.pop('partitions', [0])  # TODO: Collect from all known partitions
        batch = []
        for partition_id in partitions:
            batch.extend(self.queue.get_next_requests(max_next_requests, partition_id, **kwargs))
        self.queue_size -= len(batch)
        return batch

    def page_crawled(self, response, links):
        response.meta['state'] = States.CRAWLED
        self.states.update_cache(response)
        depth = response.meta.get('depth', 0)+1
        to_fetch = OrderedDict()
        for link in links:
            to_fetch[link.meta['fingerprint']] = link
            link.meta['depth'] = depth
        self.states.fetch(to_fetch.keys())
        self.states.set_states(links)
        unique_links = to_fetch.values()
        self.metadata.page_crawled(response, unique_links)
        self._schedule(unique_links)
        self.states.update_cache(unique_links)

    def request_error(self, request, error):
        request.meta['state'] = States.ERROR
        self.metadata.request_error(request, error)
        self.states.update_cache(request)

    def finished(self):
        return self.queue_size == 0