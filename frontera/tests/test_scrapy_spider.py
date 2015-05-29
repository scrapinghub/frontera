# -*- coding: utf-8 -*-
from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy import log, signals
from scrapy.settings import Settings
from scrapy_spider.spiders.example import MySpider


def test_scrapy_spider():
    spider = MySpider()
    settings = Settings()
    settings.setmodule("frontera.tests.scrapy_spider.settings")
    crawler = Crawler(settings)
    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
    crawler.configure()
    crawler.crawl(spider)
    crawler.start()
    log.start()
    reactor.run()

    stats = crawler.stats.spider_stats['example']
    assert stats['frontera/crawled_pages_count'] == 5
    assert spider.callback_calls > 0
