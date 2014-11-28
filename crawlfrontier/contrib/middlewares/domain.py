import re

from crawlfrontier.core.components import Middleware
from crawlfrontier.core.models import Model, Field
from crawlfrontier.utils.url import parse_domain_from_url


class Domain(Model):
    netloc = Field(order=1)
    name = Field(order=2)
    scheme = Field(order=3)
    sld = Field(order=4)
    tld = Field(order=5)
    subdomain = Field(order=6)

    def __init__(self, url, test_mode=False):
        if test_mode:
            match = re.match('([A-Z])\w+', url)
            netloc = name = match.groups()[0] if match else '?'
            scheme = sld = tld = subdomain = '-'
        else:
            netloc, name, scheme, sld, tld, subdomain = parse_domain_from_url(url)
        data = {
            'netloc': netloc,
            'name': name,
            'scheme': scheme,
            'sld': sld,
            'tld': tld,
            'subdomain': subdomain,
        }
        super(Domain, self).__init__(**data)


class DomainMiddleware(Middleware):
    component_name = 'Domain Middleware'

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
        obj.domain = Domain(obj.url, self.test_mode)
        return obj
