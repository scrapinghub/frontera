from frontera.utils.managers import FrontierManagerWrapper

from .converters import RequestConverter, ResponseConverter


class RequestsFrontierManager(FrontierManagerWrapper):
    def __init__(self, settings):
        super().__init__(settings)
        self.request_converter = RequestConverter()
        self.response_converter = ResponseConverter(self.request_converter)
