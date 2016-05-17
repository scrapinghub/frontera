# -*- coding: utf-8 -*-
from urlparse import urlparse
from frontera.core.components import States
from frontera.worker.strategies import BaseCrawlingStrategy


class CrawlingStrategy(BaseCrawlingStrategy):

    def add_seeds(self, seeds):
        for seed in seeds:
            if seed.meta['state'] is States.NOT_CRAWLED:
                seed.meta['state'] = States.QUEUED
                self.schedule(seed)

    def page_crawled(self, response, links):
        response.meta['state'] = States.CRAWLED
        for link in links:
            if link.meta['state'] is States.NOT_CRAWLED:
                link.meta['state'] = States.QUEUED
                self.schedule(link, self.get_score(link.url))

    def page_error(self, request, error):
        request.meta['state'] = States.ERROR
        self.schedule(request, score=0.0, dont_queue=True)

    def get_score(self, url):
        url_parts = urlparse(url)
        path_parts = url_parts.path.split('/')
        return 1.0 / (max(len(path_parts), 1.0) + len(url_parts.path) * 0.1)