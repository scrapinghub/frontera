import functools
import operator

from scrapy.http.request import Request as ScrapyRequest
from scrapy.http.response import Response as ScrapyResponse
from w3lib.util import to_bytes, to_unicode

from frontera.core.models import Request as FrontierRequest
from frontera.core.models import Response as FrontierResponse
from frontera.utils.converters import BaseRequestConverter, BaseResponseConverter


class RequestConverter(BaseRequestConverter):
    """Converts between frontera and Scrapy request objects"""

    def __init__(self, spider):
        self.spider = spider

    def to_frontier(self, scrapy_request):
        """request: Scrapy > Frontier"""
        if isinstance(scrapy_request.cookies, dict):
            cookies = scrapy_request.cookies
        else:
            cookies = dict(
                functools.reduce(
                    operator.iadd, [list(d.items()) for d in scrapy_request.cookies], []
                )
            )
        cb = scrapy_request.callback
        if callable(cb):
            cb = _find_method(self.spider, cb)
        eb = scrapy_request.errback
        if callable(eb):
            eb = _find_method(self.spider, eb)

        scrapy_meta = scrapy_request.meta
        meta = {}
        if b"frontier_request" in scrapy_meta:
            request = scrapy_meta[b"frontier_request"]
            if isinstance(request, FrontierRequest):
                meta = request.meta
            del scrapy_meta[b"frontier_request"]

        meta.update(
            {
                b"scrapy_callback": cb,
                b"scrapy_errback": eb,
                b"scrapy_meta": scrapy_meta,
                b"origin_is_frontier": True,
            }
        )
        if "redirect_urls" in scrapy_meta:
            meta[b"redirect_urls"] = scrapy_meta["redirect_urls"]
        return FrontierRequest(
            url=scrapy_request.url,
            method=scrapy_request.method,
            headers=scrapy_request.headers,
            cookies=cookies,
            meta=meta,
            body=scrapy_request.body,
        )

    def from_frontier(self, frontier_request):
        """request: Frontier > Scrapy"""
        cb = frontier_request.meta.get(b"scrapy_callback", None)
        if cb and self.spider:
            cb = _get_method(self.spider, cb)
        eb = frontier_request.meta.get(b"scrapy_errback", None)
        if eb and self.spider:
            eb = _get_method(self.spider, eb)
        body = frontier_request.body
        meta = frontier_request.meta.get(b"scrapy_meta", {})
        meta[b"frontier_request"] = frontier_request
        return ScrapyRequest(
            url=frontier_request.url,
            callback=cb,
            errback=eb,
            body=body,
            method=to_unicode(frontier_request.method),
            headers=frontier_request.headers,
            cookies=frontier_request.cookies,
            meta=meta,
            dont_filter=True,
        )


class ResponseConverter(BaseResponseConverter):
    """Converts between frontera and Scrapy response objects"""

    def __init__(self, spider, request_converter):
        self.spider = spider
        self._request_converter = request_converter

    def to_frontier(self, scrapy_response):
        """response: Scrapy > Frontier"""
        frontier_request = scrapy_response.meta[b"frontier_request"]
        frontier_request.meta[b"scrapy_meta"] = scrapy_response.meta
        if "redirect_urls" in scrapy_response.meta:
            frontier_request.meta[b"redirect_urls"] = scrapy_response.meta[
                "redirect_urls"
            ]
        del scrapy_response.meta[b"frontier_request"]
        return FrontierResponse(
            url=scrapy_response.url,
            status_code=scrapy_response.status,
            headers=scrapy_response.headers,
            body=scrapy_response.body,
            request=frontier_request,
        )

    def from_frontier(self, response):
        """response: Frontier > Scrapy"""
        return ScrapyResponse(
            url=response.url,
            status=response.status_code,
            headers=response.headers,
            body=response.body,
            request=self._request_converter.from_frontier(response.request),
        )


def _find_method(obj, func):
    if obj and hasattr(func, "__self__") and func.__self__ is obj:
        return to_bytes(func.__func__.__name__)
    raise ValueError(f"Function {func} is not a method of: {obj}")


def _get_method(obj, name):
    name = to_unicode(name)
    try:
        return getattr(obj, name)
    except AttributeError as e:
        raise ValueError(f"Method {name!r} not found in: {obj}") from e
