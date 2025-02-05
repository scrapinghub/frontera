import pytest

pytest.importorskip("scrapy")

from scrapy.http.request import Request as ScrapyRequest
from scrapy.http.response import Response as ScrapyResponse
from w3lib.util import to_bytes

from frontera.contrib.scrapy.converters import RequestConverter, ResponseConverter
from frontera.core.models import Request as FrontierRequest


class TestSpider:
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
    request = ScrapyRequest(
        url=url, callback=spider.callback, errback=spider.errback, body=REQUEST_BODY
    )
    request.meta[b"test_param"] = b"test_value"
    request.headers.appendlist(b"TestKey", b"test value")
    request.cookies[b"MyCookie"] = b"CookieContent"

    frontier_request = rc.to_frontier(request)
    assert frontier_request.meta[b"scrapy_callback"] == b"callback"
    assert frontier_request.meta[b"scrapy_errback"] == b"errback"
    assert frontier_request.body == to_bytes(REQUEST_BODY)
    assert frontier_request.url == url
    assert frontier_request.method == b"GET"
    assert frontier_request.headers[b"Testkey"] == b"test value"
    assert frontier_request.cookies[b"MyCookie"] == b"CookieContent"
    assert b"frontier_request" not in frontier_request.meta[b"scrapy_meta"]

    request_converted = rc.from_frontier(frontier_request)
    assert request_converted.meta[b"test_param"] == b"test_value"
    assert request_converted.body == to_bytes(REQUEST_BODY)
    assert request_converted.url == url
    assert request_converted.method == "GET"
    assert request_converted.cookies[b"MyCookie"] == b"CookieContent"
    assert request_converted.headers.get(b"Testkey") == b"test value"
    assert request_converted.callback == spider.callback
    assert request_converted.errback == spider.errback

    # Some middleware could change .meta contents
    request_converted.meta[b"middleware_stuff"] = b"appeared"

    response = ScrapyResponse(
        url=url,
        request=request_converted,
        body=RESPONSE_BODY,
        headers={b"TestHeader": b"Test value"},
    )

    frontier_response = rsc.to_frontier(response)
    assert frontier_response.body == RESPONSE_BODY
    assert frontier_response.meta[b"scrapy_meta"][b"test_param"] == b"test_value"
    assert frontier_response.meta[b"scrapy_meta"][b"middleware_stuff"] == b"appeared"
    assert frontier_response.status_code == 200
    assert b"frontier_request" not in frontier_response.meta[b"scrapy_meta"]

    response_converted = rsc.from_frontier(frontier_response)
    assert response_converted.body == RESPONSE_BODY
    assert response_converted.meta[b"test_param"] == b"test_value"
    assert response_converted.url == url
    assert response_converted.status == 200
    assert response_converted.headers[b"TestHeader"] == b"Test value"

    frontier_request = FrontierRequest(url)
    request_converted = rc.from_frontier(frontier_request)
    assert frontier_request.url == url
