from scrapy.contrib.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.contrib.linkextractors.regex import RegexLinkExtractor
from scrapy.contrib.spiders import CrawlSpider, Rule


DOMAIN = 'diffeo.com'
ALLOWED_RE = 'http://' + DOMAIN


class FallbackLinkExtractor(object):
    def __init__(self, extractors):
        self.extractors = extractors

    def extract_links(self, response):
        for lx in self.extractors:
            links = lx.extract_links(response)
            return links


class MySpider(CrawlSpider):
    name = 'recorder'
    start_urls = [
        'http://' + DOMAIN,
    ]
    allowed_domains = [DOMAIN]

    rules = [Rule(FallbackLinkExtractor([
        LxmlLinkExtractor(allow=ALLOWED_RE),
        SgmlLinkExtractor(allow=ALLOWED_RE),
        RegexLinkExtractor(allow=ALLOWED_RE),
    ]), callback='parse_page', follow=True)]

    def parse_page(self, response):
        pass
