import copy

from w3lib.util import to_bytes, to_unicode


class FrontierObject:
    def copy(self):
        return copy.copy(self)


class Request(FrontierObject):
    """
    A :class:`Request <frontera.core.models.Request>` object represents an HTTP request, which is generated for
    seeds, extracted page links and next pages to crawl. Each one should be associated to a
    :class:`Response <frontera.core.models.Response>` object when crawled.

    """

    def __init__(
        self, url, method=b"GET", headers=None, cookies=None, meta=None, body=""
    ):
        """
        :param string url: URL to send.
        :param string method: HTTP method to use.
        :param dict headers: dictionary of headers to send.
        :param dict cookies: dictionary of cookies to attach to this request.
        :param dict meta: dictionary that contains arbitrary metadata for this request, the keys must be bytes and \
        the values must be either bytes or serializable objects such as lists, tuples, dictionaries with byte type items.
        """
        self._url = to_unicode(url)
        self._method = to_bytes((method or b"GET").upper())
        self._headers = headers or {}
        self._cookies = cookies or {}
        self._meta = meta or {b"scrapy_meta": {}}
        self._body = body

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
        on the components you have enabled. The keys are bytes and the values are either bytes or serializable objects \
        such as lists, tuples, dictionaries with byte type items.
        """
        return self._meta

    @property
    def body(self):
        """
        A string representing the request body.
        """
        return self._body

    def __str__(self):
        return f"<{type(self).__name__} at 0x{id(self):0x} {self.url} meta={self.meta!s} body={self.body[:20]!s}... cookies={self.cookies!s}, headers={self.headers!s}>"

    __repr__ = __str__


class Response(FrontierObject):
    """
    A :class:`Response <frontera.core.models.Response>` object represents an HTTP response, which is usually
    downloaded (by the crawler) and sent back to the frontier for processing.

    """

    def __init__(self, url, status_code=200, headers=None, body="", request=None):
        """
        :param string url: URL of this response.
        :param int status_code: the HTTP status of the response. Defaults to 200.
        :param dict headers: dictionary of headers to send.
        :param str body: the response body.
        :param Request request: The Request object that generated this response.
        """

        self._url = to_unicode(url)
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
        except AttributeError as e:
            raise AttributeError(
                "Response.meta not available, this response is not tied to any request"
            ) from e

    def __str__(self):
        return f"<{type(self).__name__} at 0x{id(self):0x} {self.status_code} {self.url} meta={self.meta!s} body={self.body[:20]!s}... headers={self.headers!s}>"

    __repr__ = __str__
