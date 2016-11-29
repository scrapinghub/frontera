# -*- coding: utf-8 -*-
from __future__ import absolute_import

from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from tests.scrapy_spider.spiders.example import MySpider
from twisted.internet import reactor


def test_scrapy_spider():
    settings = Settings()
    settings.setmodule("tests.scrapy_spider.settings")
    crawler = Crawler(MySpider, settings=settings)
    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
    crawler.crawl()
    reactor.run()
    stats = crawler.stats.spider_stats['example']
    assert stats['frontera/crawled_pages_count'] == 5
    assert crawler.spider.callback_calls > 0
