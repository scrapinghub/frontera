from __future__ import absolute_import


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
        for url in urls:
            r = spider.make_requests_from_url(url)
            r.meta['seed'] = True
            yield r
        for r in start_requests:
            yield r

    def load_seeds(self):
        raise NotImplementedError



