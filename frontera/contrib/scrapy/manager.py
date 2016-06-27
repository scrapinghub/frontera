from __future__ import absolute_import
from frontera.utils.managers import FrontierManagerWrapper
from .converters import RequestConverter, ResponseConverter


class ScrapyFrontierManager(FrontierManagerWrapper):

    spider = None

    def set_spider(self, spider):
        assert self.spider is None, 'Spider is already set. Only one spider is supported per process.'
        self.spider = spider
        self.request_converter = RequestConverter(self.spider)
        self.response_converter = ResponseConverter(self.spider, self.request_converter)
