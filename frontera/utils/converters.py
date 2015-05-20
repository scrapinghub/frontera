class BaseRequestConverter(object):
    """Converts between frontera and XXX request objects"""
    def to_frontier(self, request):
        """request: XXX > Frontier"""
        raise NotImplementedError

    def from_frontier(self, request):
        """request: Frontier > XXX"""
        raise NotImplementedError


class BaseResponseConverter(object):
    """Converts between frontera and XXX response objects"""
    def to_frontier(self, response):
        """response: XXX > Frontier"""
        raise NotImplementedError

    def from_frontier(self, response):
        """response: Frontier > XXX"""
        raise NotImplementedError
