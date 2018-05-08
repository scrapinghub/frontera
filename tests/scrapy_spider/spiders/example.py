from __future__ import absolute_import
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.http import Request

class MySpider(CrawlSpider):
    name = 'example'
    start_urls = ['https://en.wikipedia.org/wiki/Main_Page']
    callback_calls = 0

    rules = [Rule(LinkExtractor(),
             callback='parse_page', follow=True)]

    def parse_page(self, response):
        self.callback_calls += 1
        pass

    def parse_nothing(self, response):
        pass

    def make_requests_from_url(self, url):
        return Request(url, dont_filter=True, meta={'seed':True})

    parse_start_url = parse_nothing
