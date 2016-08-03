from __future__ import absolute_import
from six.moves.urllib import parse
from w3lib.util import to_native_str


def parse_url(url, encoding=None):
    """Return urlparsed url from the given argument (which could be an already
    parsed url)
    """
    return url if isinstance(url, parse.ParseResult) else \
        parse.urlparse(to_native_str(url))


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
