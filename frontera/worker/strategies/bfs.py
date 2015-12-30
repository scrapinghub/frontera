# -*- coding: utf-8 -*-
from urlparse import urlparse
from frontera.core.components import States, BaseCrawlingStrategy


class CrawlingStrategy(BaseCrawlingStrategy):
    def add_seeds(self, seeds):
        scores = {}
        for seed in seeds:
            if seed.meta['state'] is None:
                scores[seed.meta['fingerprint']] = 1.0
                seed.meta['state'] = States.QUEUED
        return scores

    def page_crawled(self, response, links):
        scores = {}
        response.meta['state'] = States.CRAWLED
        for link in links:
            if link.meta['state'] is None:
                scores[link.meta['fingerprint']] = self.get_score(link.url)
                link.meta['state'] = States.QUEUED
        return scores

    def page_error(self, request, error):
        request.meta['state'] = States.ERROR
        return {request.meta['fingerprint']: 0.0}

    def get_score(self, url):
        url_parts = urlparse(url)
        path_parts = url_parts.path.split('/')
        return 1.0 / (max(len(path_parts), 1.0) + len(url_parts.path) * 0.1)