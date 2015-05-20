from requests.models import Request as RequestsRequest
from requests.models import Response as RequestsResponse

from frontera.core.models import Request as FrontierRequest
from frontera.core.models import Response as FrontierResponse
from frontera.utils.converters import BaseRequestConverter, BaseResponseConverter


class RequestConverter(BaseRequestConverter):
    """Converts between frontera and Requests request objects"""
    def to_frontier(self, request):
        """request: Requests > Frontier"""
        return FrontierRequest(url=request.url,
                               method=request.method,
                               headers=request.headers,
                               cookies=request.cookies if hasattr(request, 'cookies') else {})

    def from_frontier(self, request):
        """request: Frontier > Scrapy"""
        return RequestsRequest(url=request.url,
                               method=request.method,
                               headers=request.headers,
                               cookies=request.cookies)


class ResponseConverter(BaseResponseConverter):
    """Converts between frontera and Scrapy response objects"""
    def __init__(self, request_converter):
        self._request_converter = request_converter

    def to_frontier(self, response):
        """response: Scrapy > Frontier"""
        return FrontierResponse(url=response.url,
                                status_code=response.status_code,
                                headers=response.headers,
                                body=response.text,
                                request=self._request_converter.to_frontier(response.request))

    def from_frontier(self, response):
        """response: Frontier > Scrapy"""
        raise NotImplementedError
