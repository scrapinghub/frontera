from crawlfrontier.core.components import Middleware
from crawlfrontier.exceptions import NotConfigured
from crawlfrontier.utils.url import canonicalize_url
from crawlfrontier.utils.misc import load_object


class BaseFingerprintMiddleware(Middleware):
    component_name = 'Base Fingerprint Middleware'
    fingerprint_function_name = ''

    def __init__(self, manager):
        fingerprint_function_name = manager.settings.get(self.fingerprint_function_name, None)
        if not fingerprint_function_name:
            manager.logger.frontier.warning('Missing function "%s" in settings' % self.fingerprint_function_name)
            raise NotConfigured
        self.fingerprint_function = load_object(fingerprint_function_name)

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def add_seeds(self, links):
        for link in links:
            self._add_fingerprint(link)
        return links

    def page_crawled(self, page, links):
        for link in links:
            self._add_fingerprint(link)
        return self._add_fingerprint(page)

    def page_crawled_error(self, page, error):
        return self._add_fingerprint(page)

    def get_page(self, link):
        return self._add_fingerprint(link)

    def _add_fingerprint(self, obj):
        raise NotImplementedError


class UrlFingerprintMiddleware(BaseFingerprintMiddleware):
    component_name = 'URL Fingerprint Middleware'
    fingerprint_function_name = 'URL_FINGERPRINT_FUNCTION'

    def _add_fingerprint(self, obj):
        if hasattr(obj, 'url'):
            obj.fingerprint = self.fingerprint_function(canonicalize_url(obj.url))
        return obj


class DomainFingerprintMiddleware(BaseFingerprintMiddleware):
    component_name = 'Domain Fingerprint Middleware'
    fingerprint_function_name = 'DOMAIN_FINGERPRINT_FUNCTION'

    def _add_fingerprint(self, obj):
        if hasattr(obj, 'domain'):
            obj.domain.fingerprint = self.fingerprint_function(obj.domain.name)
        return obj
