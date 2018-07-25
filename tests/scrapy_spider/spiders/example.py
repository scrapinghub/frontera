from __future__ import absolute_import
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule


class MySpider(CrawlSpider):
    name = 'example'
    callback_calls = 0

    rules = [Rule(LinkExtractor(),
             callback='parse_page', follow=True)]

    def parse_page(self, response):
        self.callback_calls += 1
        pass

    def parse_nothing(self, response):
        pass

    parse_start_url = parse_nothing
