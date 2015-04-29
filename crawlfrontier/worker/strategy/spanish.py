# -*- coding: utf-8 -*-
from urlparse import urlparse

class CrawlStrategy(object):
    def __init__(self):
        self.domain_white_list = ["es.wikipedia.org", "www.dmoz.org"]

    def add_seeds(self, seeds):
        pass

    def page_crawled(self, response, links):
        pass

    def finished(self):
        return False

    def get_score(self, url):
        url_parts = urlparse(url)

        if not url_parts.hostname.endswith(".es") and not url_parts.hostname in self.domain_white_list:
            return None

        path_parts = url_parts.path.split('/')
        return 1.0 / len(path_parts)