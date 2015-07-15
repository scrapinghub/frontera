# -*- coding: utf-8 -*-

from frontera.contrib.scrapy.converters import RequestConverter, ResponseConverter
from scrapy.http.request import Request as ScrapyRequest
from scrapy.http.response import Response as ScrapyResponse
from frontera.core.models import Request as FrontierRequest


class TestSpider(object):
    def callback(self):
        pass

    def errback(self):
        pass


def test_request_response_converters():
    spider = TestSpider()
    rc = RequestConverter(spider)
    rsc = ResponseConverter(spider, rc)

    url = "http://test.com/test?param=123"
    request = ScrapyRequest(url=url, callback=spider.callback, errback=spider.errback)
    request.meta['test_param'] = 'test_value'
    request.headers.appendlist("TestKey", "test value")
    request.cookies['MyCookie'] = 'CookieContent'

    frontier_request = rc.to_frontier(request)
    assert frontier_request.meta['scrapy_callback'] == 'callback'
    assert frontier_request.meta['scrapy_errback'] == 'errback'
    assert frontier_request.url == url
    assert frontier_request.method == 'GET'
    assert frontier_request.headers['Testkey'] == 'test value'
    assert frontier_request.cookies['MyCookie'] == 'CookieContent'
    assert 'frontier_request' not in frontier_request.meta['scrapy_meta']

    request_converted = rc.from_frontier(frontier_request)
    assert request_converted.meta['test_param'] == 'test_value'
    assert request_converted.url == url
    assert request_converted.method == 'GET'
    assert request_converted.cookies['MyCookie'] == 'CookieContent'
    assert request_converted.headers.get('Testkey') == 'test value'

    # Some middleware could change .meta contents
    request_converted.meta['middleware_stuff'] = 'appeared'

    response = ScrapyResponse(url=url, request=request_converted, headers={'TestHeader': 'Test value'})
    frontier_response = rsc.to_frontier(response)
    assert frontier_response.meta['scrapy_meta']['test_param'] == 'test_value'
    assert frontier_response.meta['scrapy_meta']['middleware_stuff'] == 'appeared'
    assert frontier_response.status_code == 200
    assert 'frontier_request' not in frontier_response.meta['scrapy_meta']

    response_converted = rsc.from_frontier(frontier_response)
    assert response_converted.meta['test_param'] == 'test_value'
    assert response_converted.url == url
    assert response_converted.status == 200
    assert response_converted.headers['TestHeader'] == 'Test value'

    frontier_request = FrontierRequest(url)
    request_converted = rc.from_frontier(frontier_request)
    assert frontier_request.url == url
