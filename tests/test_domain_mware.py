from __future__ import absolute_import
import unittest
from frontera.contrib.middlewares.domain import DomainMiddleware
from frontera.core.manager import FrontierManager
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
            {b'name': 'example.com', b'netloc': 'example.com', b'scheme': 'http',
             b'sld': '', b'subdomain': '', b'tld': ''},
            {b'name': 'www.google.com', b'netloc': 'www.google.com', b'scheme': 'https',
             b'sld': '', b'subdomain': '', b'tld': ''},
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
            {b'name': 'example.com', b'netloc': 'example.com', b'scheme': 'http',
             b'sld': 'example', b'subdomain': '', b'tld': 'com'},
            {b'name': 'google.com', b'netloc': 'www.google.com', b'scheme': 'https',
             b'sld': 'google', b'subdomain': 'www', b'tld': 'com'},
        ]
        self.assertEquals(expected, [r.meta[b'domain'] for r in result])
