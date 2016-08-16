# -*- coding: utf-8 -*-
from __future__ import absolute_import
from six.moves.urllib.parse import urlparse
from frontera.core.components import States
from frontera.worker.strategies import BaseCrawlingStrategy


class CrawlingStrategy(BaseCrawlingStrategy):

    def add_seeds(self, seeds):
        for seed in seeds:
            if seed.meta[b'state'] is States.NOT_CRAWLED:
                seed.meta[b'state'] = States.QUEUED
                self.schedule(seed)

    def page_crawled(self, response):
        response.meta[b'state'] = States.CRAWLED

    def links_extracted(self, request, links):
        for link in links:
            if link.meta[b'state'] is States.NOT_CRAWLED:
                link.meta[b'state'] = States.QUEUED
                self.schedule(link, self.get_score(link.url))

    def page_error(self, request, error):
        request.meta[b'state'] = States.ERROR
        self.schedule(request, score=0.0, dont_queue=True)

    def get_score(self, url):
        url_parts = urlparse(url)
        path_parts = url_parts.path.split('/')
        return 1.0 / (max(len(path_parts), 1.0) + len(url_parts.path) * 0.1)
