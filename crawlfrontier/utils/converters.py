class BaseRequestConverter(object):
    """Converts between crawlfrontier and XXX request objects"""
    @classmethod
    def to_frontier(cls, request):
        """request: XXX > Frontier"""
        raise NotImplementedError

    @classmethod
    def from_frontier(cls, request):
        """request: Frontier > XXX"""
        raise NotImplementedError


class BaseResponseConverter(object):
    """Converts between crawlfrontier and XXX response objects"""
    @classmethod
    def to_frontier(cls, response):
        """response: XXX > Frontier"""
        raise NotImplementedError

    @classmethod
    def from_frontier(cls, response):
        """response: Frontier > XXX"""
        raise NotImplementedError
