from scrapy.contrib.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.contrib.linkextractors.regex import RegexLinkExtractor
from scrapy.contrib.spiders import CrawlSpider, Rule


class FallbackLinkExtractor(object):
    def __init__(self, extractors):
        self.extractors = extractors

    def extract_links(self, response):
        for lx in self.extractors:
            links = lx.extract_links(response)
            return links


class MySpider(CrawlSpider):
    name = 'example'
    start_urls = ['http://scrapinghub.com']
    callback_calls = 0

    rules = [Rule(FallbackLinkExtractor([
        LxmlLinkExtractor(),
        RegexLinkExtractor(),
    ]), callback='parse_page', follow=True)]

    def parse_page(self, response):
        self.callback_calls += 1
        pass

    def parse_nothing(self, response):
        pass

    parse_start_url = parse_nothing
