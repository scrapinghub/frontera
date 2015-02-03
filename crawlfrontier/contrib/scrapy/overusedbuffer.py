from scrapy.utils.httpobj import urlparse_cached
from scrapy.resolver import dnscache

from crawlfrontier.core import OverusedBuffer


class OverusedBufferScrapy(OverusedBuffer):

    def _get_key(self, request, type):
        key = urlparse_cached(request).hostname or ''
        if type == 'ip':
            key = dnscache.get(key, key)
        return key
