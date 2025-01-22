from scrapy.utils.httpobj import urlparse_cached
from scrapy.resolver import dnscache

from frontera.core import OverusedBuffer


class OverusedBufferScrapy(OverusedBuffer):
    """
    Scrapy optimized version of OverusedBuffer. Url parsing and dns resolving are made using Scrapy primitives.
    """

    def _get_key(self, request, type):
        key = urlparse_cached(request).hostname or ''
        if type == 'ip':
            key = dnscache.get(key, key)
        return key
