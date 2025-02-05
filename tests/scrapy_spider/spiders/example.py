from scrapy import Spider


class MySpider(Spider):
    name = "example"
    start_urls = ["data:,"]
    callback_calls = 0

    def parse(self, response):
        self.callback_calls += 1
