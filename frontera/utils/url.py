import urlparse
import urllib
import cgi
import hashlib
from six import moves
from w3lib.util import unicode_to_str


# Python 2.x urllib.always_safe become private in Python 3.x;
# its content is copied here
_ALWAYS_SAFE_BYTES = (b'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                      b'abcdefghijklmnopqrstuvwxyz'
                      b'0123456789' b'_.-')


_reserved = b';/?:@&=+$|,#'  # RFC 3986 (Generic Syntax)
_unreserved_marks = b"-_.!~*'()"  # RFC 3986 sec 2.3
_safe_chars = _ALWAYS_SAFE_BYTES + b'%' + _reserved + _unreserved_marks


def parse_url(url, encoding=None):
    """Return urlparsed url from the given argument (which could be an already
    parsed url)
    """
    return url if isinstance(url, urlparse.ParseResult) else \
        urlparse.urlparse(unicode_to_str(url, encoding))


def parse_domain_from_url(url):
    """
    Extract domain info from a passed url.
    Examples:
    -------------------------------------------------------------------------------------------------------
     URL                       NETLOC              NAME            SCHEME    SLD         TLD     SUBDOMAIN
    -------------------------------------------------------------------------------------------------------
     http://www.google.com     www.google.com      google.com      http      google      com     www
     http://docs.google.com    docs.google.com     google.com      http      google      com     docs
     https://google.es/mail    google.es           google.es       https     google      es
    -------------------------------------------------------------------------------------------------------
    """
    import tldextract
    extracted = tldextract.extract(url)
    scheme, _, _, _, _, _ = parse_url(url)

    sld = extracted.domain
    tld = extracted.suffix
    subdomain = extracted.subdomain
    name = '.'.join([sld, tld]) if tld else sld
    netloc = '.'.join([subdomain, name]) if subdomain else name

    return netloc, name, scheme, sld, tld, subdomain


def parse_domain_from_url_fast(url):
    """
    Extract domain info from a passed url, without analyzing subdomains and tld
    """
    result = parse_url(url)
    return result.netloc, result.hostname, result.scheme, "", "", ""


def safe_url_string(url, encoding='utf8'):
    """Convert the given url into a legal URL by escaping unsafe characters
    according to RFC-3986.

    If a unicode url is given, it is first converted to str using the given
    encoding (which defaults to 'utf-8'). When passing a encoding, you should
    use the encoding of the original page (the page from which the url was
    extracted from).

    Calling this function on an already "safe" url will return the url
    unmodified.

    Always returns a str.
    """
    s = unicode_to_str(url, encoding)
    return moves.urllib.parse.quote(s, _safe_chars)


def _unquotepath(path):
    for reserved in ('2f', '2F', '3f', '3F'):
        path = path.replace('%' + reserved, '%25' + reserved.upper())
    return urllib.unquote(path)


def canonicalize_url(url, keep_blank_values=True, keep_fragments=False):
    """Canonicalize the given url by applying the following procedures:

    - sort query arguments, first by key, then by value
    - percent encode paths and query arguments. non-ASCII characters are
      percent-encoded using UTF-8 (RFC-3986)
    - normalize all spaces (in query arguments) '+' (plus symbol)
    - normalize percent encodings case (%2f -> %2F)
    - remove query arguments with blank values (unless keep_blank_values is True)
    - remove fragments (unless keep_fragments is True)

    The url passed can be a str or unicode, while the url returned is always a
    str.

    For examples see the tests in scrapy.tests.test_utils_url
    """

    scheme, netloc, path, params, query, fragment = parse_url(url)
    keyvals = cgi.parse_qsl(query, keep_blank_values)
    keyvals.sort()
    query = urllib.urlencode(keyvals)
    path = safe_url_string(_unquotepath(path)) or '/'
    fragment = '' if not keep_fragments else fragment
    return urlparse.urlunparse((scheme, netloc.lower(), path, params, query, fragment))


