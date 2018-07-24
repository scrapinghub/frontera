# -*- coding: utf-8 -*-
from __future__ import absolute_import
from frontera.core.components import States
from frontera.strategy import BaseCrawlingStrategy


class BreadthFirstCrawlingStrategy(BaseCrawlingStrategy):
    def read_seeds(self, fh):
        for url in fh:
            url = url.strip()
            req = self.create_request(url)
            self.refresh_states(req)
            if req.meta[b'state'] is States.NOT_CRAWLED:
                req.meta[b'state'] = States.QUEUED
                req.meta[b'depth'] = 0
                self.schedule(req)

    def page_crawled(self, response):
        response.meta[b'state'] = States.CRAWLED

    def filter_extracted_links(self, request, links):
        return links

    def links_extracted(self, request, links):
        for link in links:
            link.meta[b'depth'] = request.meta[b'depth'] + 1
            if link.meta[b'state'] is States.NOT_CRAWLED:
                link.meta[b'state'] = States.QUEUED
                self.schedule(link, self.get_score(link))

    def request_error(self, request, error):
        request.meta[b'state'] = States.ERROR
        self.schedule(request, score=0.0, dont_queue=True)

    def get_score(self, link):
        depth = float(link.meta[b'depth'])
        return 1.0 - (depth / (depth + 1.0))


class DepthFirstCrawlingStrategy(BreadthFirstCrawlingStrategy):
    def get_score(self, link):
        depth = float(link.meta[b'depth'])
        return depth / (depth + 1.0)