from __future__ import absolute_import
from frontera.core.components import Middleware
from frontera.exceptions import NotConfigured
from w3lib.url import canonicalize_url
from frontera.utils.misc import load_object


class BaseFingerprintMiddleware(Middleware):
    component_name = 'Base Fingerprint Middleware'
    fingerprint_function_name = ''

    def __init__(self, manager):
        fingerprint_function_name = manager.settings.get(self.fingerprint_function_name, None)
        if not fingerprint_function_name:
            raise NotConfigured
        self.fingerprint_function = load_object(fingerprint_function_name)

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass

    def add_seeds(self, seeds):
        for seed in seeds:
            self._add_fingerprint(seed)
        return seeds

    def page_crawled(self, response):
        return self._add_fingerprint(response)

    def links_extracted(self, request, links):
        for link in links:
            self._add_fingerprint(link)
        return self._add_fingerprint(request)

    def request_error(self, request, error):
        return self._add_fingerprint(request)

    def create_request(self, request):
        return self._add_fingerprint(request)

    def _add_fingerprint(self, obj):
        raise NotImplementedError


class UrlFingerprintMiddleware(BaseFingerprintMiddleware):
    """
    This :class:`Middleware <frontera.core.components.Middleware>` will add a ``fingerprint`` field for every
    :attr:`Request.meta <frontera.core.models.Request.meta>` and
    :attr:`Response.meta <frontera.core.models.Response.meta>` if is activated.

    Fingerprint will be calculated from object ``URL``, using the function defined in
    :setting:`URL_FINGERPRINT_FUNCTION` setting.
    You can write your own fingerprint calculation function and use by changing this setting.
    The fingerprint must be bytes.

    An example for a :class:`Request <frontera.core.models.Request>` object::

        >>> request.url
        'http//www.scrapinghub.com:8080'

        >>> request.meta['fingerprint']
        '60d846bc2969e9706829d5f1690f11dafb70ed18'

    """

    component_name = 'URL Fingerprint Middleware'
    fingerprint_function_name = 'URL_FINGERPRINT_FUNCTION'

    def _get_fingerprint(self, url):
        return self.fingerprint_function(canonicalize_url(url))

    def _add_fingerprint(self, obj):
        obj.meta[b'fingerprint'] = self._get_fingerprint(obj.url)
        if b'redirect_urls' in obj.meta:
            obj.meta[b'redirect_fingerprints'] = [self._get_fingerprint(url) for url in obj.meta[b'redirect_urls']]
        return obj


class DomainFingerprintMiddleware(BaseFingerprintMiddleware):
    """
    This :class:`Middleware <frontera.core.components.Middleware>` will add a ``fingerprint`` field for every
    :attr:`Request.meta <frontera.core.models.Request.meta>` and
    :attr:`Response.meta <frontera.core.models.Response.meta>` ``domain`` fields if is activated.

    Fingerprint will be calculated from object ``URL``, using the function defined in
    :setting:`DOMAIN_FINGERPRINT_FUNCTION` setting.
    You can write your own fingerprint calculation function and use by changing this setting.
    The fingerprint must be bytes

    An example for a :class:`Request <frontera.core.models.Request>` object::

        >>> request.url
        'http//www.scrapinghub.com:8080'

        >>> request.meta['domain']
        {
            "fingerprint": "5bab61eb53176449e25c2c82f172b82cb13ffb9d",
            "name": "scrapinghub.com",
            "netloc": "www.scrapinghub.com",
            "scheme": "http",
            "sld": "scrapinghub",
            "subdomain": "www",
            "tld": "com"
        }

    """

    component_name = 'Domain Fingerprint Middleware'
    fingerprint_function_name = 'DOMAIN_FINGERPRINT_FUNCTION'

    def _add_fingerprint(self, obj):
        if b'domain' in obj.meta and b'name' in obj.meta[b'domain']:
            obj.meta[b'domain'][b'fingerprint'] = self.fingerprint_function(obj.meta[b'domain'][b'name'])
        if b'redirect_domains' in obj.meta:
            for domain in obj.meta[b'redirect_domains']:
                domain[b'fingerprint'] = self.fingerprint_function(domain[b'name'])
        return obj
