import pytest

pytest.importorskip("tldextract")

import unittest

from frontera.utils.url import (
    parse_domain_from_url,
    parse_domain_from_url_fast,
    parse_url,
)

simple_url = "http://www.example.com"
complete_url = (
    "http://username:password@www.example.com:80/some/page/do?a=1&b=2&c=3#frag"
)


class TestParseUrl(unittest.TestCase):
    def test_simple_url(self):
        self.assertEqual(
            parse_url(simple_url), ("http", "www.example.com", "", "", "", "")
        )

    def test_complete_url(self):
        self.assertEqual(
            parse_url(complete_url),
            (
                "http",
                "username:password@www.example.com:80",
                "/some/page/do",
                "",
                "a=1&b=2&c=3",
                "frag",
            ),
        )

    def test_already_parsed(self):
        result = parse_url(simple_url)
        self.assertEqual(parse_url(result), result)


class TestParseDomainFromUrl(unittest.TestCase):
    def test_simple_url(self):
        self.assertEqual(
            parse_domain_from_url(simple_url),
            ("www.example.com", "example.com", "http", "example", "com", "www"),
        )

    def test_complete_url(self):
        self.assertEqual(
            parse_domain_from_url(complete_url),
            ("www.example.com", "example.com", "http", "example", "com", "www"),
        )

    def test_missing_tld(self):
        self.assertEqual(
            parse_domain_from_url("http://www.example"),
            ("www.example", "example", "http", "example", "", "www"),
        )

    def test_missing_subdomain(self):
        self.assertEqual(
            parse_domain_from_url("https://example.com"),
            ("example.com", "example.com", "https", "example", "com", ""),
        )

    def test_missing_scheme(self):
        self.assertEqual(
            parse_domain_from_url("www.example.com"),
            ("www.example.com", "example.com", "", "example", "com", "www"),
        )


class TestParseDomainFromUrlFast(unittest.TestCase):
    def test_simple_url(self):
        self.assertEqual(
            parse_domain_from_url_fast(simple_url),
            ("www.example.com", "www.example.com", "http", "", "", ""),
        )

    def test_complete_url(self):
        self.assertEqual(
            parse_domain_from_url_fast(complete_url),
            (
                "username:password@www.example.com:80",
                "www.example.com",
                "http",
                "",
                "",
                "",
            ),
        )
