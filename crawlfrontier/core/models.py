import copy


class FrontierObject(object):
    def copy(self):
        return copy.deepcopy(self)


class Request(FrontierObject):
    """
    ....

    :param url: URL to send.
    :param method: HTTP method to use.
    :param headers: dictionary of headers to send.
    :param cookies: dictionary of cookies to attach to this request.
    :param meta: dictionary that contains arbitrary metadata for this request.
    """
    def __init__(self, url, method='GET', headers=None, cookies=None, meta=None):
        self.url = url
        self.method = str(method).upper()
        self.headers = headers or {}
        self.cookies = cookies or {}
        self.meta = meta or {}

    def __str__(self):
        return "<%s at 0x%0x %s>" % (type(self).__name__, id(self), self.url)

    __repr__ = __str__


class Response(FrontierObject):
    """
    ....

    :param url: URL of this response.
    :param status_code: the HTTP status of the response. Defaults to 200.
    :param headers: dictionary of headers to send.
    :param body: the response body.
    :param request: The Request object that generated this response.
    """
    def __init__(self, url, status_code=200, headers=None, body='', request=None):
        self.url = url
        self.status_code = int(status_code)
        self.headers = headers or {}
        self.body = body
        self.request = request

    @property
    def meta(self):
        try:
            return self.request.meta
        except AttributeError:
            raise AttributeError("Response.meta not available, this response " \
                                 "is not tied to any request")

    def __str__(self):
        return "<%s at 0x%0x %s %s>" % (type(self).__name__, id(self), self.status_code, self.url)

    __repr__ = __str__
