import re

from crawlfrontier.core.components import Middleware
from crawlfrontier.utils.url import parse_domain_from_url


def parse_domain_info(url, test_mode=False):
    if test_mode:
        match = re.match('([A-Z])\w+', url)
        netloc = name = match.groups()[0] if match else '?'
        scheme = sld = tld = subdomain = '-'
    else:
        netloc, name, scheme, sld, tld, subdomain = parse_domain_from_url(url)
    return {
        'netloc': netloc,
        'name': name,
        'scheme': scheme,
        'sld': sld,
        'tld': tld,
        'subdomain': subdomain,
    }

class DomainMiddleware(Middleware):
    component_name = 'Domain Middleware'

    def __init__(self, manager):
        self.manager = manager

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def add_seeds(self, seeds):
        for seed in seeds:
            self._add_domain(seed)
        return seeds

    def page_crawled(self, response, links):
        for link in links:
            self._add_domain(link)
        return self._add_domain(response)

    def request_error(self, request, error):
        return self._add_domain(request)

    def _add_domain(self, obj):
        obj.meta['domain'] = parse_domain_info(obj.url, self.manager.test_mode)
        return obj
