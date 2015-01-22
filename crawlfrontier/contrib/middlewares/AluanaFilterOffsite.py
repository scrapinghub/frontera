"""
Offsite Spider Middleware

See documentation in docs/topics/spider-middleware.rst
"""

import re

from scrapy import signals
from scrapy.http import Request
from scrapy.utils.httpobj import urlparse_cached
from scrapy import log
from crawlfrontier import Middleware
import tldextract


class OffsiteMiddleware(Middleware):

    def __init__(self, manager):
        self.manager = manager
        self.allowed_domains = ['amazonaws.com']
        self.host_regex = self.get_host_regex()
        self.domains_seen = set()

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)


    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass

    def add_seeds(self, seeds):
        return seeds

    def request_error(self, request, error):
        pass


    def page_crawled(self, response, links):
        try:
            if response.request.isSeed == True:
                self.allowed_domains.append(response.request.meta['domain']['netloc'])
                self.host_regex = self.get_host_regex()
        except Exception as e:
            pass
        for link in links:
            fingerprint = link.meta['fingerprint']
            if fingerprint not in self.manager.backend.requests:
                should = self.should_follow(link)
                if not should:
                    self.manager.backend.requests[link.meta['fingerprint']] = link
                    # log.msg(message='Filtered offsite '+str(link.url),
                    #     level=log.DEBUG, domain=str(response.meta['domain']['netloc']), url=str(link.url),
                    #     module='offsite_filter', filtered=True)
        return response


    def should_follow(self, request):
        regex = self.host_regex
        # hostname can be None for wrong urls (like javascript links)
        host = urlparse_cached(request).hostname or ''
        return bool(regex.search(host))

    def get_host_regex(self):
        """Override this method to implement a different offsite policy"""
        allowed_domains = self.allowed_domains
        if not allowed_domains:
            return re.compile('') # allow all by default
        regex = r'^(.*\.)?(%s)$' % '|'.join(re.escape(d) for d in allowed_domains if d is not None)
        return re.compile(regex)

