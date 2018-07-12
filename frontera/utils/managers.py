from __future__ import absolute_import
from frontera.core.manager import LocalFrontierManager, SpiderFrontierManager
from .converters import BaseRequestConverter, BaseResponseConverter


class FrontierManagerWrapper(object):
    def __init__(self, settings, manager=None):
        if manager is None:
            manager = LocalFrontierManager if settings.get("LOCAL_MODE") is True else SpiderFrontierManager
        self.manager = manager.from_settings(settings)
        self.request_converter = None
        self.response_converter = None

    def start(self):
        if not hasattr(self, 'request_converter'):
            raise NotImplementedError("Request converter should be instantiated in subclass")
        if not hasattr(self, 'response_converter'):
            raise NotImplementedError("Response converter should be instantiated in subclass")
        assert isinstance(self.request_converter, BaseRequestConverter), 'request_converter ' \
                                                                         'must be instance of BaseRequestConverter'
        assert isinstance(self.response_converter, BaseResponseConverter), 'response_converter ' \
                                                                           'must be instance of BaseResponseConverter'
        self.manager.start()

    def stop(self):
        self.manager.stop()

    def get_next_requests(self, max_next_requests=0, **kwargs):
        frontier_requests = self.manager.get_next_requests(max_next_requests=max_next_requests, **kwargs)
        return [self.request_converter.from_frontier(frontier_request) for frontier_request in frontier_requests]

    def page_crawled(self, response):
        self.manager.page_crawled(self.response_converter.to_frontier(response))

    def links_extracted(self, request, links):
        frontier_links = [self.request_converter.to_frontier(link) for link in links]
        self.manager.links_extracted(request=self.request_converter.to_frontier(request),
                                     links=frontier_links)

    def request_error(self, request, error):
        self.manager.request_error(request=self.request_converter.to_frontier(request),
                                   error=error)

    def finished(self):
        return self.manager.finished
