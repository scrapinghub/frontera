from __future__ import absolute_import
import unittest
from frontera.contrib.middlewares.domain import DomainMiddleware
from frontera.core.models import Request


class FakeManager(object):
    settings = {}
    test_mode = False


class DomainMiddlewareTest(unittest.TestCase):
    def setUp(self):
        self.fake_manager = FakeManager()

    def test_create(self):
        DomainMiddleware(self.fake_manager)

    def test_should_parse_domain_info(self):
        seeds = [
            Request('http://example.com'),
            Request('https://www.google.com'),
        ]

        mware = DomainMiddleware(self.fake_manager)
        result = mware.add_seeds(seeds)

        self.assertEquals(len(result), len(seeds))

        for r in result:
            self.assertIn(b'domain', r.meta, 'Missing domain info for %r' % r)

        expected = [
            {b'name': b'example.com', b'netloc': b'example.com', b'scheme': b'http',
             b'sld': b'', b'subdomain': b'', b'tld': b''},
            {b'name': b'www.google.com', b'netloc': b'www.google.com', b'scheme': b'https',
             b'sld': b'', b'subdomain': b'', b'tld': b''},
        ]
        self.assertEquals(expected, [r.meta[b'domain'] for r in result])

    def test_should_parse_tldextract_extra_domain_info(self):
        seeds = [
            Request('http://example.com'),
            Request('https://www.google.com'),
        ]

        self.fake_manager.settings = {'TLDEXTRACT_DOMAIN_INFO': True}

        mware = DomainMiddleware(self.fake_manager)
        result = mware.add_seeds(seeds)

        self.assertEquals(len(result), len(seeds))

        for r in result:
            self.assertIn(b'domain', r.meta, 'Missing domain info for %r' % r)

        expected = [
            {b'name': b'example.com', b'netloc': b'example.com', b'scheme': b'http',
             b'sld': b'example', b'subdomain': b'', b'tld': b'com'},
            {b'name': b'google.com', b'netloc': b'www.google.com', b'scheme': b'https',
             b'sld': b'google', b'subdomain': b'www', b'tld': b'com'},
        ]
        self.assertEquals(expected, [r.meta[b'domain'] for r in result])
