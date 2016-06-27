# -*- coding: utf-8 -*-
from __future__ import absolute_import
from frontera.core.models import Request, Response
from w3lib.url import safe_url_string
import six


url1 = u'http://www.example.com'
url2 = u'https://example.com/some/pageÅ'  # unicode with non-ascii character.
url3 = b'https://example.com/some/page\xc3\x85'  # utf-8 encoded bytes
url4 = b'https://example.com/some/page\xc5'  # latin-1 encoded bytes


class TestRequest(object):

    def test(self):
        r = Request(url1)
        assert r.url == 'http://www.example.com'
        assert r.method == 'GET'
        assert r.cookies == {}
        assert r.headers == {}
        assert r.meta == {'scrapy_meta': {}}
        assert r.body == ''

    def test_non_ascii_unicode(self):
        r = Request(url2)
        assert r.url == safe_url_string(url2)

    def test_non_ascii_bytes(self):
        r = Request(url3)
        assert r.url == safe_url_string(url2)

    def test_non_default_encoding(self):
        r = Request(url4, method='DELETE', headers={'header': 'test'}, cookies={'cookie': 'test'},
                    meta={'test_value': 'test'}, body='test', encoding='latin1')
        assert r.url == safe_url_string(url2, 'latin1')
        assert r.method == 'DELETE'
        assert r.cookies == {'cookie': 'test'}
        assert r.headers == {'header': 'test'}
        assert r.meta == {'test_value': 'test'}
        assert r.body == 'test'


class TestResponse(object):

    def test(self):
        r = Response(url1)
        assert r.url == 'http://www.example.com'
        assert r.status_code == 200
        assert r.headers == {}
        assert r.body == ''
        assert r.request is None

    def test_non_ascii_unicode(self):
        r = Response(url2)
        assert r.url == safe_url_string(url2)

    def test_non_ascii_bytes(self):
        r = Response(url3)
        assert r.url == safe_url_string(url2)

    def test_non_default_encoding(self):
        request = Request('http://www.example.com')
        r = Response(url4, status_code=100, headers={'header': 'test'}, body='test',
                     request=request, encoding='latin1')
        assert r.url == safe_url_string(url2, 'latin1')
        assert r.status_code == 100
        assert r.headers == {'header': 'test'}
        assert r.body == 'test'
        assert r.request == request
