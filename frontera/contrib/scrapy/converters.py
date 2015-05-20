from scrapy.http.request import Request as ScrapyRequest
from scrapy.http.response import Response as ScrapyResponse

from frontera.core.models import Request as FrontierRequest
from frontera.core.models import Response as FrontierResponse
from frontera.utils.converters import BaseRequestConverter, BaseResponseConverter


class RequestConverter(BaseRequestConverter):
    """Converts between frontera and Scrapy request objects"""
    def __init__(self, spider):
        self.spider = spider

    def to_frontier(self, request):
        """request: Scrapy > Frontier"""
        if isinstance(request.cookies, dict):
            cookies = request.cookies
        else:
            cookies = dict(sum([d.items() for d in request.cookies], []))
        cb = request.callback
        if callable(cb):
            cb = _find_method(self.spider, cb)
        eb = request.errback
        if callable(eb):
            eb = _find_method(self.spider, eb)
        meta = {
            'scrapy_callback': cb,
            'scrapy_errback': eb,
            'origin_is_frontier': True,
        }
        meta.update(request.meta or {})
        return FrontierRequest(url=request.url,
                               method=request.method,
                               headers=request.headers,
                               cookies=cookies,
                               meta=meta)

    def from_frontier(self, request):
        """request: Frontier > Scrapy"""
        meta = {
            'frontier_request': request
        }
        meta.update(request.meta or {})
        cb = meta.get('scrapy_callback', None)
        if cb and self.spider:
            cb = _get_method(self.spider, cb)
        eb = meta.get('scrapy_errback', None)
        if eb and self.spider:
            eb = _get_method(self.spider, eb)
        return ScrapyRequest(url=request.url,
                             callback=cb,
                             errback=eb,
                             method=request.method,
                             headers=request.headers,
                             cookies=request.cookies,
                             meta=meta,
                             dont_filter=True)


class ResponseConverter(BaseResponseConverter):
    """Converts between frontera and Scrapy response objects"""
    def __init__(self, spider, request_converter):
        self.spider = spider
        self._request_converter = request_converter

    def to_frontier(self, response):
        """response: Scrapy > Frontier"""
        frontier_response = FrontierResponse(url=response.url,
                                             status_code=response.status,
                                             headers=response.headers,
                                             body=response.body,
                                             request=response.meta['frontier_request'])
        frontier_response.meta.update(response.meta)
        return frontier_response

    def from_frontier(self, response):
        """response: Frontier > Scrapy"""
        return ScrapyResponse(url=response.url,
                              status=response.status_code,
                              headers=response.headers,
                              body=response.body,
                              request=self._request_converter.from_frontier(response.request))

def _find_method(obj, func):
    if obj and hasattr(func, 'im_self') and func.im_self is obj:
        return func.im_func.__name__
    else:
        raise ValueError("Function %s is not a method of: %s" % (func, obj))

def _get_method(obj, name):
    name = str(name)
    try:
        return getattr(obj, name)
    except AttributeError:
        raise ValueError("Method %r not found in: %s" % (name, obj))