# -*- coding: utf-8 -*-
from __future__ import absolute_import

import sys

from scrapy.core.spidermw import SpiderMiddlewareManager
from scrapy.http import Request, Response
from scrapy.http.request import Request as ScrapyRequest
from scrapy.http.response import Response as ScrapyResponse
from scrapy.spiders import Spider
from scrapy.utils.test import get_crawler
from twisted.internet.defer import Deferred
from twisted.trial import unittest
from w3lib.util import to_bytes

from frontera.contrib.scrapy.converters import (RequestConverter,
                                                ResponseConverter)
from frontera.core.models import Request as FrontierRequest
from frontera.contrib.scrapy.schedulers.frontier import FronteraScheduler


class TestSpider(object):
    def callback(self):
        pass

    def errback(self):
        pass


REQUEST_BODY = b"BodyContent"
RESPONSE_BODY = b"This is a fake response body"


def test_request_response_converters():
    spider = TestSpider()
    rc = RequestConverter(spider)
    rsc = ResponseConverter(spider, rc)

    url = "http://test.com/test?param=123"
    request = ScrapyRequest(url=url, callback=spider.callback, errback=spider.errback,
                            body=REQUEST_BODY)
    request.meta[b'test_param'] = b'test_value'
    request.headers.appendlist(b"TestKey", b"test value")
    request.cookies[b'MyCookie'] = b'CookieContent'

    frontier_request = rc.to_frontier(request)
    assert frontier_request.meta[b'scrapy_callback'] == b'callback'
    assert frontier_request.meta[b'scrapy_errback'] == b'errback'
    assert frontier_request.body == to_bytes(REQUEST_BODY)
    assert frontier_request.url == url
    assert frontier_request.method == b'GET'
    assert frontier_request.headers[b'Testkey'] == b'test value'
    assert frontier_request.cookies[b'MyCookie'] == b'CookieContent'
    assert b'frontier_request' not in frontier_request.meta[b'scrapy_meta']

    request_converted = rc.from_frontier(frontier_request)
    assert request_converted.meta[b'test_param'] == b'test_value'
    assert request_converted.body == to_bytes(REQUEST_BODY)
    assert request_converted.url == url
    assert request_converted.method == 'GET'
    assert request_converted.cookies[b'MyCookie'] == b'CookieContent'
    assert request_converted.headers.get(b'Testkey') == b'test value'
    assert request_converted.callback == spider.callback
    assert request_converted.errback == spider.errback

    # Some middleware could change .meta contents
    request_converted.meta[b'middleware_stuff'] = b'appeared'

    response = ScrapyResponse(url=url, request=request_converted, body=RESPONSE_BODY,
                              headers={b'TestHeader': b'Test value'})

    frontier_response = rsc.to_frontier(response)
    assert frontier_response.body == RESPONSE_BODY
    assert frontier_response.meta[b'scrapy_meta'][b'test_param'] == b'test_value'
    assert frontier_response.meta[b'scrapy_meta'][b'middleware_stuff'] == b'appeared'
    assert frontier_response.status_code == 200
    assert b'frontier_request' not in frontier_response.meta[b'scrapy_meta']

    response_converted = rsc.from_frontier(frontier_response)
    assert response_converted.body == RESPONSE_BODY
    assert response_converted.meta[b'test_param'] == b'test_value'
    assert response_converted.url == url
    assert response_converted.status == 200
    assert response_converted.headers[b'TestHeader'] == b'Test value'

    frontier_request = FrontierRequest(url)
    request_converted = rc.from_frontier(frontier_request)
    assert frontier_request.url == url


class TestFronteraMiddlewaresWithScrapy(unittest.TestCase):

    def setUp(self):
        class TestSpider(Spider):
            name = 'test'

        self.spider = TestSpider
        scrapy_default_middlewares = {
            'scrapy.spidermiddlewares.referer.RefererMiddleware': 700
        }

        # monkey patch SPIDER_MIDDLEWARES_BASE to include only referer middleware
        sys.modules['scrapy.settings.default_settings'].SPIDER_MIDDLEWARES_BASE = scrapy_default_middlewares

        custom_settings = {
            'SPIDER_MIDDLEWARES': {'frontera.contrib.scrapy.middlewares.schedulers.SchedulerSpiderMiddleware': 1000}
        }
        crawler = get_crawler(self.spider, custom_settings)
        self.add_frontera_scheduler(crawler)
        self.smw = SpiderMiddlewareManager.from_crawler(crawler)

    @staticmethod
    def add_frontera_scheduler(crawler):
        scheduler = FronteraScheduler(crawler)

        # mock these functions
        scheduler.frontier.page_crawled = lambda x: x
        scheduler.frontier.links_extracted = lambda x, y: x
        scheduler.stats_manager.add_crawled_page = lambda x, y: x

        class Engine(object):
            def __init__(self, scheduler):
                self.slot = type('slot', (object,), {})
                self.slot.scheduler = scheduler

        crawler.engine = Engine(scheduler)

    def test_frontera_scheduler_spider_middleware_with_referer_middleware(self):

        def request_callback(response):
            yield Request('http://frontera.org')

        req = Request(
            url='http://www.scrapy.org',
            callback=request_callback,
            meta={b'frontier_request': FrontierRequest('http://www.scrapy.org')}
        )

        res = Response(url='http://www.scrapy.org', request=req)

        def call_request_callback(result, request, spider):
            dfd = Deferred()
            dfd.addCallback(request.callback)
            return dfd

        def test_middleware_output(result):
            out = list(result)
            self.assertEquals(len(out), 1)
            self.assertIsInstance(out[0], Request)
            self.assertIn('Referer', out[0].headers)
            self.assertEquals(out[0].headers['Referer'], to_bytes(res.url))

        def test_failure(failure):
            # work around for test to fail with detailed traceback
            self._observer._errors.append(failure)

        dfd = self.smw.scrape_response(call_request_callback, res, req, self.spider)

        dfd.addCallback(test_middleware_output)
        dfd.addErrback(test_failure)

        dfd.callback(res)
