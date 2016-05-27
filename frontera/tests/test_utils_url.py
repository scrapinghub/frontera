import unittest
from frontera.utils.url import parse_url, parse_domain_from_url, \
    parse_domain_from_url_fast, safe_url_string, canonicalize_url


simple_url = 'http://www.example.com'
complete_url = 'http://username:password@www.example.com:80/some/page/do?a=1&b=2&c=3#frag'


class TestParseUrl(unittest.TestCase):

    def test_simple_url(self):
        self.assertEqual(parse_url(simple_url),
                         ('http', 'www.example.com', '', '', '', ''))

    def test_complete_url(self):
        self.assertEqual(parse_url(complete_url),
                         ('http', 'username:password@www.example.com:80',
                          '/some/page/do', '', 'a=1&b=2&c=3', 'frag'))

    def test_already_parsed(self):
        result = parse_url(simple_url)
        self.assertEqual(parse_url(result), result)


class TestParseDomainFromUrl(unittest.TestCase):

    def test_simple_url(self):
        self.assertEqual(parse_domain_from_url(simple_url),
                         ('www.example.com', 'example.com', 'http', 'example', 'com', 'www'))

    def test_complete_url(self):
        self.assertEqual(parse_domain_from_url(complete_url),
                         ('www.example.com', 'example.com', 'http', 'example', 'com', 'www'))

    def test_missing_tld(self):
        self.assertEqual(parse_domain_from_url('http://www.example'),
                         ('www.example', 'example', 'http', 'example', '', 'www'))

    def test_missing_subdomain(self):
        self.assertEqual(parse_domain_from_url('https://example.com'),
                         ('example.com', 'example.com', 'https', 'example', 'com', ''))

    def test_missing_scheme(self):
        self.assertEqual(parse_domain_from_url('www.example.com'),
                         ('www.example.com', 'example.com', '', 'example', 'com', 'www'))


class TestParseDomainFromUrlFast(unittest.TestCase):

    def test_simple_url(self):
        self.assertEqual(parse_domain_from_url_fast(simple_url),
                         ('www.example.com', 'www.example.com', 'http', '', '', ''))

    def test_complete_url(self):
        self.assertEqual(parse_domain_from_url_fast(complete_url),
                         ('username:password@www.example.com:80', 'www.example.com', 'http', '', '', ''))


class TestSafeUrlString(unittest.TestCase):

    def test_safe_url_string(self):
        # Motoko Kusanagi (Cyborg from Ghost in the Shell)
        motoko = u'\u8349\u8599 \u7d20\u5b50'
        self.assertEqual(safe_url_string(motoko),  # note the %20 for space
                         '%E8%8D%89%E8%96%99%20%E7%B4%A0%E5%AD%90')
        self.assertEqual(safe_url_string(motoko),
                         safe_url_string(safe_url_string(motoko)))
        self.assertEqual(safe_url_string(u'\xa9'),  # copyright symbol
                         '%C2%A9')
        self.assertEqual(safe_url_string(u'\xa9', 'iso-8859-1'),
                         '%A9')
        self.assertEqual(safe_url_string("http://www.scrapy.org/"),
                         'http://www.scrapy.org/')

        alessi = u'/ecommerce/oggetto/Te \xf2/tea-strainer/1273'

        self.assertEqual(safe_url_string(alessi),
                         '/ecommerce/oggetto/Te%20%C3%B2/tea-strainer/1273')

        self.assertEqual(safe_url_string("http://www.example.com/test?p(29)url(http://www.another.net/page)"),
                         "http://www.example.com/test?p(29)url(http://www.another.net/page)")
        self.assertEqual(safe_url_string("http://www.example.com/Brochures_&_Paint_Cards&PageSize=200"),
                         "http://www.example.com/Brochures_&_Paint_Cards&PageSize=200")

        safeurl = safe_url_string(u"http://www.example.com/\xa3", encoding='latin-1')
        self.assert_(isinstance(safeurl, str))
        self.assertEqual(safeurl, "http://www.example.com/%A3")

        safeurl = safe_url_string(u"http://www.example.com/\xa3", encoding='utf-8')
        self.assert_(isinstance(safeurl, str))
        self.assertEqual(safeurl, "http://www.example.com/%C2%A3")


class TestCanonicalizeUrl(unittest.TestCase):

    def test_simple_case(self):
        self.assertEqual(canonicalize_url("http://www.example.com/"),
                         "http://www.example.com/")

    def test_returns_str(self):
        assert isinstance(canonicalize_url(u"http://www.example.com"), str)

    def test_append_missing_path(self):
        self.assertEqual(canonicalize_url("http://www.example.com"),
                         "http://www.example.com/")

    def test_typical_usage(self):
        self.assertEqual(canonicalize_url("http://www.example.com/do?a=1&b=2&c=3"),
                         "http://www.example.com/do?a=1&b=2&c=3")
        self.assertEqual(canonicalize_url("http://www.example.com/do?c=1&b=2&a=3"),
                         "http://www.example.com/do?a=3&b=2&c=1")
        self.assertEqual(canonicalize_url("http://www.example.com/do?&a=1"),
                         "http://www.example.com/do?a=1")

    def test_sorting(self):
        self.assertEqual(canonicalize_url("http://www.example.com/do?c=3&b=5&b=2&a=50"),
                         "http://www.example.com/do?a=50&b=2&b=5&c=3")

    def test_keep_blank_values(self):
        self.assertEqual(canonicalize_url("http://www.example.com/do?b=&a=2", keep_blank_values=False),
                         "http://www.example.com/do?a=2")
        self.assertEqual(canonicalize_url("http://www.example.com/do?b=&a=2"),
                         "http://www.example.com/do?a=2&b=")
        self.assertEqual(canonicalize_url("http://www.example.com/do?b=&c&a=2", keep_blank_values=False),
                         "http://www.example.com/do?a=2")
        self.assertEqual(canonicalize_url("http://www.example.com/do?b=&c&a=2"),
                         "http://www.example.com/do?a=2&b=&c=")

        self.assertEqual(canonicalize_url(u'http://www.example.com/do?1750,4'),
                         'http://www.example.com/do?1750%2C4=')

    def test_spaces(self):
        self.assertEqual(canonicalize_url("http://www.example.com/do?q=a space&a=1"),
                         "http://www.example.com/do?a=1&q=a+space")
        self.assertEqual(canonicalize_url("http://www.example.com/do?q=a+space&a=1"),
                         "http://www.example.com/do?a=1&q=a+space")
        self.assertEqual(canonicalize_url("http://www.example.com/do?q=a%20space&a=1"),
                         "http://www.example.com/do?a=1&q=a+space")

    def test_normalize_percent_encoding_in_path(self):
        self.assertEqual(canonicalize_url("http://www.example.com/a%a3do"),
                         "http://www.example.com/a%A3do"),

    def test_normalize_percent_encoding_in_query_arguments(self):
        self.assertEqual(canonicalize_url("http://www.example.com/do?k=b%a3"),
                         "http://www.example.com/do?k=b%A3")

    def test_non_ascii_percent_encoding_in_path(self):
        self.assertEqual(canonicalize_url("http://www.example.com/a do?a=1"),
                         "http://www.example.com/a%20do?a=1"),
        self.assertEqual(canonicalize_url("http://www.example.com/a %20do?a=1"),
                         "http://www.example.com/a%20%20do?a=1"),
        self.assertEqual(canonicalize_url("http://www.example.com/a do\xc2\xa3.html?a=1"),
                         "http://www.example.com/a%20do%C2%A3.html?a=1")

    def test_non_ascii_percent_encoding_in_query_argument(self):
        self.assertEqual(canonicalize_url(u"http://www.example.com/do?price=\xa3500&a=5&z=3"),
                         u"http://www.example.com/do?a=5&price=%C2%A3500&z=3")
        self.assertEqual(canonicalize_url("http://www.example.com/do?price=\xc2\xa3500&a=5&z=3"),
                         "http://www.example.com/do?a=5&price=%C2%A3500&z=3")
        self.assertEqual(canonicalize_url("http://www.example.com/do?price(\xc2\xa3)=500&a=1"),
                         "http://www.example.com/do?a=1&price%28%C2%A3%29=500")

    def test_auth_and_ports(self):
        self.assertEqual(canonicalize_url(u"http://user:pass@www.example.com:81/do?now=1"),
                         u"http://user:pass@www.example.com:81/do?now=1")

    def test_remove_fragments(self):
        self.assertEqual(canonicalize_url(u"http://user:pass@www.example.com/do?a=1#frag"),
                         u"http://user:pass@www.example.com/do?a=1")
        self.assertEqual(canonicalize_url(u"http://user:pass@www.example.com/do?a=1#frag", keep_fragments=True),
                         u"http://user:pass@www.example.com/do?a=1#frag")

    def test_dont_convert_safe_chars(self):
        self.assertEqual(canonicalize_url(
            "http://www.simplybedrooms.com/White-Bedroom-Furniture/Bedroom-Mirror:-Josephine-Cheval-Mirror.html"),
            "http://www.simplybedrooms.com/White-Bedroom-Furniture/Bedroom-Mirror:-Josephine-Cheval-Mirror.html")

    def test_safe_characters_unicode(self):
        # urllib.quote uses a mapping cache of encoded characters. when parsing
        # an already percent-encoded url, it will fail if that url was not
        # percent-encoded as utf-8, that's why canonicalize_url must always
        # convert the urls to string. the following test asserts that
        # functionality.
        self.assertEqual(canonicalize_url(u'http://www.example.com/caf%E9-con-leche.htm'),
                         'http://www.example.com/caf%E9-con-leche.htm')

    def test_domains_are_case_insensitive(self):
        self.assertEqual(canonicalize_url("http://www.EXAMPLE.com/"),
                         "http://www.example.com/")

    def test_quoted_slash_and_question_sign(self):
        self.assertEqual(canonicalize_url("http://foo.com/AC%2FDC+rocks%3f/?yeah=1"),
                         "http://foo.com/AC%2FDC+rocks%3F/?yeah=1")
        self.assertEqual(canonicalize_url("http://foo.com/AC%2FDC/"),
                         "http://foo.com/AC%2FDC/")
