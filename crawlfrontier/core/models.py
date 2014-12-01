import copy


class FrontierObject(object):
    def __init__(self, url, meta=None):
        self.url = url
        self.meta = meta or {}

    def copy(self):
        return copy.deepcopy(self)


class Request(FrontierObject):
    def __init__(self, url, meta=None):
        super(Request, self).__init__(url=url, meta=meta)

    def __str__(self):
        return "<%s at 0x%0x %s>" % (type(self).__name__, id(self), self.url)

    __repr__ = __str__


class Response(FrontierObject):
    def __init__(self, url, status_code, request, meta=None):
        super(Response, self).__init__(url=url, meta=meta)
        self.status_code = status_code
        self.request = request

    def __str__(self):
        return "<%s at 0x%0x %s %s>" % (type(self).__name__, id(self), self.status_code, self.url)

    __repr__ = __str__
