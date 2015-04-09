# -*- coding: utf-8 -*-
from crawlfrontier.utils.url import parse_url

class DiscoveryScorer(object):
    def add_seed(self, request):
        return 1.0

    def page_crawled(self, response, link):
        url_parts = parse_url(link.url)
        path_parts = url_parts.path.split('/')
        return 1.0 / len(path_parts)

    def page_error(self, request, error):
        return request.meta['score']

