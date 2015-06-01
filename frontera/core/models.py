import copy


class FrontierObject(object):
    def copy(self):
        return copy.copy(self)


class Request(FrontierObject):
    """
    A :class:`Request <frontera.core.models.Request>` object represents an HTTP request, which is generated for
    seeds, extracted page links and next pages to crawl. Each one should be associated to a
    :class:`Response <frontera.core.models.Response>` object when crawled.

    """
    def __init__(self, url, method='GET', headers=None, cookies=None, meta=None):
        """
        :param string url: URL to send.
        :param string method: HTTP method to use.
        :param dict headers: dictionary of headers to send.
        :param dict cookies: dictionary of cookies to attach to this request.
        :param dict meta: dictionary that contains arbitrary metadata for this request.
        """
        self._url = url
        self._method = str(method).upper()
        self._headers = headers or {}
        self._cookies = cookies or {}
        self._meta = meta or {'scrapy_meta': {}}

    @property
    def url(self):
        """
        A string containing the URL of this request.
        """
        return self._url

    @property
    def method(self):
        """
        A string representing the HTTP method in the request. This is guaranteed to be uppercase.
        Example: ``GET``, ``POST``, ``PUT``, etc
        """
        return self._method

    @property
    def headers(self):
        """
        A dictionary which contains the request headers.
        """
        return self._headers

    @property
    def cookies(self):
        """
        Dictionary of cookies to attach to this request.
        """
        return self._cookies

    @property
    def meta(self):
        """
        A dict that contains arbitrary metadata for this request. This dict is empty for new Requests, and is usually
        populated by different Frontera components (middlewares, etc). So the data contained in this dict depends
        on the components you have enabled.
        """
        return self._meta

    def __str__(self):
        return "<%s at 0x%0x %s>" % (type(self).__name__, id(self), self.url)

    __repr__ = __str__


class Response(FrontierObject):
    """
    A :class:`Response <frontera.core.models.Response>` object represents an HTTP response, which is usually
    downloaded (by the crawler) and sent back to the frontier for processing.

    """

    def __init__(self, url, status_code=200, headers=None, body='', request=None):
        """
        :param string url: URL of this response.
        :param int status_code: the HTTP status of the response. Defaults to 200.
        :param dict headers: dictionary of headers to send.
        :param dict body: the response body.
        :param dict request: The Request object that generated this response.
        """

        self._url = url
        self._status_code = int(status_code)
        self._headers = headers or {}
        self._body = body
        self._request = request

    @property
    def url(self):
        """
        A string containing the URL of the response.
        """
        return self._url

    @property
    def status_code(self):
        """
        An integer representing the HTTP status of the response. Example: ``200``, ``404``, ``500``.
        """
        return self._status_code

    @property
    def headers(self):
        """
        A dictionary object which contains the response headers.
        """
        return self._headers

    @property
    def body(self):
        """
        A str containing the body of this Response.
        """
        return self._body

    @property
    def request(self):
        """
        The :class:`Request <frontera.core.models.Request>` object that generated this response.
        """
        return self._request

    @property
    def meta(self):
        """
        A shortcut to the :attr:`Request.meta <frontera.core.models.Request.meta>` attribute of the
        :attr:`Response.request <frontera.core.models.Response.request>` object (ie. self.request.meta).
        """
        try:
            return self.request.meta
        except AttributeError:
            raise AttributeError("Response.meta not available, this response "
                                 "is not tied to any request")

    def __str__(self):
        return "<%s at 0x%0x %s %s>" % (type(self).__name__, id(self), self.status_code, self.url)

    __repr__ = __str__
