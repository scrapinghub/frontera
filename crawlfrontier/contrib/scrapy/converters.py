from scrapy.http.request import Request as ScrapyRequest
from scrapy.http.response import Response as ScrapyResponse

from crawlfrontier.core.models import Request as FrontierRequest
from crawlfrontier.core.models import Response as FrontierResponse
from crawlfrontier.utils.converters import BaseRequestConverter, BaseResponseConverter


class RequestConverter(BaseRequestConverter):
    """Converts between crawlfrontier and Scrapy request objects"""
    @classmethod
    def to_frontier(cls, request):
        """request: Scrapy > Frontier"""
        if isinstance(request.cookies, dict):
            cookies = request.cookies
        else:
            cookies = dict(sum([d.items() for d in request.cookies], []))
        meta = {
            'scrapy_callback': request.callback,
            'origin_is_frontier': True,
        }
        meta.update(request.meta or {})
        return FrontierRequest(url=request.url,
                               method=request.method,
                               headers=request.headers,
                               cookies=cookies,
                               meta=meta)

    @classmethod
    def from_frontier(cls, request):
        """request: Frontier > Scrapy"""
        meta = {
            'frontier_request': request
        }
        meta.update(request.meta or {})
        return ScrapyRequest(url=request.url,
                             callback=meta.get('scrapy_callback', None),
                             method=request.method,
                             headers=request.headers,
                             cookies=request.cookies,
                             meta=meta,
                             dont_filter=True)


class ResponseConverter(BaseResponseConverter):
    """Converts between crawlfrontier and Scrapy response objects"""
    @classmethod
    def to_frontier(cls, response):
        """response: Scrapy > Frontier"""
        return FrontierResponse(url=response.url,
                                status_code=response.status,
                                headers=response.headers,
                                body=response.body,
                                request=response.meta['frontier_request'])

    @classmethod
    def from_frontier(cls, response):
        """response: Frontier > Scrapy"""
        return ScrapyResponse(url=response.url,
                              status=response.status,
                              headers=response.headers,
                              body=response.body,
                              request=RequestConverter.from_frontier(response.request))