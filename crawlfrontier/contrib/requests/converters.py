from requests.models import Request as RequestsRequest
from requests.models import Response as RequestsResponse

from crawlfrontier.core.models import Request as FrontierRequest
from crawlfrontier.core.models import Response as FrontierResponse
from crawlfrontier.utils.converters import BaseRequestConverter, BaseResponseConverter


class RequestConverter(BaseRequestConverter):
    """Converts between crawlfrontier and Requests request objects"""
    @classmethod
    def to_frontier(cls, request):
        """request: Requests > Frontier"""
        return FrontierRequest(url=request.url,
                               method=request.method,
                               headers=request.headers,
                               cookies=request.cookies if hasattr(request, 'cookies') else {})

    @classmethod
    def from_frontier(cls, request):
        """request: Frontier > Scrapy"""
        return RequestsRequest(url=request.url,
                               method=request.method,
                               headers=request.headers,
                               cookies=request.cookies)


class ResponseConverter(BaseResponseConverter):
    """Converts between crawlfrontier and Scrapy response objects"""
    @classmethod
    def to_frontier(cls, response):
        """response: Scrapy > Frontier"""
        return FrontierResponse(url=response.url,
                                status_code=response.status_code,
                                headers=response.headers,
                                body=response.text,
                                request=RequestConverter.to_frontier(response.request))

    @classmethod
    def from_frontier(cls, response):
        """response: Frontier > Scrapy"""
        raise NotImplementedError
