from crawlfrontier.core.manager import FrontierManager
from converters import BaseRequestConverter, BaseResponseConverter


class FrontierManagerWrapper(object):
    request_converter_class = None
    response_converter_class = None

    def __init__(self, settings, **kwargs):
        assert self.request_converter_class, 'request_converter_class not defined'
        assert self.response_converter_class, 'response_converter_class not defined'
        assert issubclass(self.request_converter_class, BaseRequestConverter), 'request_converter_class ' \
                                                                               'must subclass RequestConverter'
        assert issubclass(self.response_converter_class, BaseResponseConverter), 'response_converter_class ' \
                                                                                 'must subclass RequestConverter'
        self.request_converter = self.request_converter_class
        self.response_converter = self.response_converter_class
        self.manager = FrontierManager.from_settings(settings, **kwargs)

    def start(self, **kwargs):
        self.manager.start(**kwargs)

    def stop(self, **kwargs):
        self.manager.stop(**kwargs)

    def add_seeds(self, seeds):
        frontier_seeds = [self.request_converter.to_frontier(seed) for seed in seeds]
        self.manager.add_seeds(seeds=frontier_seeds)

    def get_next_requests(self, max_next_requests=0):
        frontier_requests = self.manager.get_next_requests(max_next_requests=max_next_requests)
        return [self.request_converter.from_frontier(frontier_request) for frontier_request in frontier_requests]

    def page_crawled(self, response, links=None):
        frontier_response = self.response_converter.to_frontier(response)
        frontier_links = [self.request_converter.to_frontier(link) for link in links]
        self.manager.page_crawled(response=frontier_response,
                                  links=frontier_links)

    def request_error(self, request, error):
        self.manager.request_error(request=self.request_converter.to_frontier(request),
                                   error=error)
