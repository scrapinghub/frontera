"""
RefererMiddleware: populates Request referer field, based on the Response which
originated it.
"""

from scrapy.http import Request
from scrapy.exceptions import NotConfigured
from crawlfrontier import Middleware


class RefererMiddleware(Middleware):

    def __init__(self, manager):
        self.manager = manager

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass

    def add_seeds(self, seeds):
        return seeds

    def page_crawled(self, response, links):
        for link in links:
            link.headers.setdefault('Referer', response.url)
        return response

    def request_error(self, request, error):
        return request