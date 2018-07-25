# -*- coding: utf-8 -*-
from __future__ import absolute_import
from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy import signals
from scrapy.settings import Settings as ScrapySettings
from tests.scrapy_spider.spiders.example import MySpider
from frontera.settings import Settings as FronteraSettings
from frontera.utils import add_seeds
import pytest
from os import remove
from os.path import exists


@pytest.fixture()
def seeds_file():
    fh = open("seeds.txt", "w")
    fh.write("https://en.wikipedia.org/wiki/Main_Page")
    fh.close()
    yield "seeds.txt"
    remove("seeds.txt")


@pytest.fixture()
def db_file(request):
    def rm_file():
        if exists("test.db"):
            remove("test.db")
    rm_file()
    request.addfinalizer(rm_file)

@pytest.mark.skip("throws ReactorNotRestartable and requires some planning")
def test_scrapy_spider(seeds_file, db_file):
    fs = FronteraSettings(module="tests.scrapy_spider.frontera.settings")
    add_seeds.run_add_seeds(fs, seeds_file)
    settings = ScrapySettings()
    settings.setmodule("tests.scrapy_spider.settings")
    crawler = Crawler(MySpider, settings=settings)
    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
    crawler.crawl()
    reactor.run()
    stats = crawler.stats.spider_stats['example']
    assert stats['frontera/crawled_pages_count'] == 5
    assert crawler.spider.callback_calls > 0
