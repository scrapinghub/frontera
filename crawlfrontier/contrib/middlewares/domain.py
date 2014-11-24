import re

from crawlfrontier.core.components import Middleware
from crawlfrontier.core.models import Model
from crawlfrontier.utils.url import parse_domain_from_url


class Domain(Model):
    component_name = 'Domain Middleware'

    def __init__(self, netloc, name, scheme, sld, tld, subdomain):
        self.netloc = netloc
        self.name = name
        self.scheme = scheme
        self.sld = sld
        self.tld = tld
        self.subdomain = subdomain

    @property
    def _name(self):
        return self.name


class DomainMiddleware(Middleware):

    def __init__(self, manager):
        self.test_mode = manager.test_mode

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def add_seeds(self, links):
        for link in links:
            self._add_domain(link)
        return links

    def page_crawled(self, page, links):
        for link in links:
            self._add_domain(link)
        return self._add_domain(page)

    def page_crawled_error(self, page, error):
        return self._add_domain(page)

    def get_page(self, link):
        return self._add_domain(link)

    def _add_domain(self, obj):
        setattr(obj, 'domain', self._get_domain_from_url(obj.url))
        return obj

    def _get_domain_from_url(self, url):
        if self.test_mode:
            match = re.match('([A-Z])\w+', url)
            netloc = name = match.groups()[0] if match else '?'
            scheme = sld = tld = subdomain = '-'
        else:
            netloc, name, scheme, sld, tld, subdomain = parse_domain_from_url(url)

        return Domain(name=name, netloc=netloc, scheme=scheme, sld=sld, tld=tld, subdomain=subdomain)
