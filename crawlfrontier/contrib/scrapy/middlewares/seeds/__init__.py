from scrapy.http.request import Request


class SeedLoader(object):
    def __init__(self, crawler):
        self.crawler = crawler
        self.configure(crawler.settings)

    def configure(self, settings):
        raise NotImplementedError

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_start_requests(self, start_requests, spider):
        urls = [url for url in self.load_seeds() if not url.startswith('#')]
        return [spider.make_requests_from_url(url) for url in urls]

    def load_seeds(self):
        raise NotImplementedError



