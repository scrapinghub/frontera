from crawlfrontier.core.manager import FrontierManager
from crawlfrontier.contrib.converters.scrapy import RequestConverter, ResponseConverter


class ScrapyFrontierManager(object):

    def __init__(self, settings):
        self.manager = FrontierManager.from_settings(settings)

    def start(self):
        self.manager.start()

    def stop(self):
        self.manager.stop()

    def add_seeds(self, scrapy_seeds):
        frontier_seeds = [RequestConverter.to_frontier(scrapy_seed) for scrapy_seed in scrapy_seeds]
        self.manager.add_seeds(seeds=frontier_seeds)

    def get_next_requests(self, max_next_requests=0):
        frontier_requests = self.manager.get_next_requests(max_next_requests=max_next_requests)
        return [RequestConverter.from_frontier(frontier_request) for frontier_request in frontier_requests]

    def page_crawled(self, scrapy_response, scrapy_links=None):
        frontier_response = ResponseConverter.to_frontier(scrapy_response)
        frontier_links = [RequestConverter.to_frontier(scrapy_link) for scrapy_link in scrapy_links]
        self.manager.page_crawled(response=frontier_response,
                                  links=frontier_links)

    def request_error(self, scrapy_request, error):
        self.manager.request_error(request=RequestConverter.to_frontier(scrapy_request),
                                   error=error)
