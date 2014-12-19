from crawlfrontier.utils.managers import FrontierManagerWrapper
from converters import RequestConverter, ResponseConverter


class RequestsFrontierManager(FrontierManagerWrapper):
    request_converter_class = RequestConverter
    response_converter_class = ResponseConverter
