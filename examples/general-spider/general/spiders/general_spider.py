from scrapy.http import Request
from scrapy.http.response.html import HtmlResponse
from scrapy.linkextractors import LinkExtractor
from scrapy.spider import Spider


class GeneralSpider(Spider):
    name = "general"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.le = LinkExtractor()

    def parse(self, response):
        if not isinstance(response, HtmlResponse):
            return

        for link in self.le.extract_links(response):
            r = Request(url=link.url)
            r.meta.update(link_text=link.text)
            yield r
