# -*- coding: utf-8 -*-
from __future__ import print_function, absolute_import

from math import floor
from time import time
from zlib import crc32

import codecs
import logging
import random
import six
import six.moves.urllib.robotparser as robotparser
from frontera.core.components import States, DomainMetadata
from frontera.strategy import BaseCrawlingStrategy
from frontera.strategy.discovery.sitemap import parse_sitemap
from publicsuffix import PublicSuffixList
from six.moves.urllib.parse import urljoin, urlsplit
from w3lib.util import to_bytes, to_native_str


MAX_SITEMAPS = 100
MAX_SUBDOMAINS = 10
MAX_DOMAINS_REDIRECTS_STORE = 100
SITEMAP_DOWNLOAD_MAXSIZE = 50 * 1024 * 1024  # 50MB
DEFAULT_HOME_PATHS = [
    '/', 'index.html', 'index.htm',
    'default.htm', 'default.html',
]
DEFAULT_HEADERS = {b'Accept-Language:': b'en-US,en'}


def is_home_page_url(url):
    parsed_url = urlsplit(url)
    # XXX prevent exceeding hard limit with parametrized home links
    return not parsed_url.query and (
        not parsed_url.path or parsed_url.path in DEFAULT_HOME_PATHS)


def is_accessible_domain(domain):
    return 'fatal_error' not in domain


def is_domain_to_ignore(domain, max_pages):
    return (not is_accessible_domain(domain) or 'banned' in domain or
            domain.setdefault('queued_pages', 0) >= max_pages)


def justify_request_score_by_hostname(hostname, score):
    hostname_crc = crc32(to_bytes(hostname, 'utf-8', 'ignore'))
    perhost_score = abs(hostname_crc / 2147483647.0)
    return floor(perhost_score * 10) / 10 + max(0.01, score - 0.01) / 10.0


def update_domain_with_parser_data(domain, parser, url, body=None):
    """Helper to update a domain metadata in the cache.
    Body param is optional and can be used to drop the field.
    """
    domain['_rp'] = parser
    domain['rp_timestamp'] = int(time())
    domain['rp_url'] = url
    domain['rp_body'] = body
    if body is None:
        del domain['rp_body']


def consume_randomly(iterable):
    """Helper to consume from iterable in random fashion.
    Note that it converts an iterable to a list and keeps it in memory.
    """
    data = list(iterable)
    size = len(data)
    while size:
        index = random.randrange(size)
        yield data[index]
        data[index] = data[size - 1]
        size -= 1


def is_valid_robotstxt(lines):
    for raw_line in lines:
        line = raw_line.strip(u'\ufeff').lower()  # '\xef\xbb\xbf' in case of bytes
        if line and not line.startswith("#"):
            if line.startswith("user-agent:") or line.startswith("sitemap:"):
                return True
            else:
                return False
    return False


class DomainCacheProxyWeb(DomainMetadata):
    def __init__(self, domain_metadata):
        self._domain_metadata = domain_metadata
        self._set_fields = {'subdomains', 'redirect_from', 'redirect_to'}

    def __setitem__(self, key, value):
        self._domain_metadata[key] = value

    def __getitem__(self, key):
        value = self._domain_metadata[key]
        for k, v in six.iteritems(value):
            if k in self._set_fields:
                value[k] = set(value[k])
        if 'rp_url' in value and 'rp_body' in value:
            value['_rp'] = robotparser.RobotFileParser(value['rp_url'])
            value['_rp'].parse(value['rp_body'].splitlines())
        return value

    def __contains__(self, key):
        return key in self._domain_metadata

    def __delitem__(self, key):
        del self._domain_metadata[key]

    def flush(self):
        if hasattr(self._domain_metadata, "flush"):
            self._domain_metadata.flush()

    def setdefault(self, key, default=None):
        if hasattr(self._domain_metadata, "setdefault"):
            return self._domain_metadata.setdefault(key, default)
        try:
            value = self[key]
        except KeyError:
            value = default
            self[key] = value
        return value


class Discovery(BaseCrawlingStrategy):

    def __init__(self, manager, args, mb_stream, states_context):
        self.logger = logging.getLogger("discovery")
        backend = manager.backend
        self.domain_cache = DomainCacheProxyWeb(backend.domain_metadata)

        try:
            psl_file = codecs.open("public_suffix_list.dat", encoding='utf8')
        except IOError:
            self.logger.exception("Please get the public suffix file from https://publicsuffix.org/")
            raise
        self._suffix_list = PublicSuffixList(psl_file)
        self._states_ctx = states_context
        self.states = backend.states

        self.user_agent = to_native_str(manager.settings.get('USER_AGENT'))
        self.max_pages = int(manager.settings.get('DISCOVERY_MAX_PAGES'))
        super(Discovery, self).__init__(manager, args, mb_stream, states_context)

    @classmethod
    def from_worker(cls, manager, args, mb_scheduler, states_context):
        return cls(manager, args, mb_scheduler, states_context)

    def close(self):
        self.domain_cache.flush()
        super(Discovery, self).close()

    # Handling seeds logic

    def read_seeds(self, stream):
        processed, scheduled = 0, 0
        requests = []
        for line in stream:
            url = to_native_str(line.strip())
            if url.startswith("#"):
                continue
            if not url.startswith("http"):
                url = "http://" + url + "/"
            try:
                request = self.create_request(url, meta={b'home': True}, headers=DEFAULT_HEADERS)
                requests.append(request)
                if len(requests) % 40000 == 0:
                    scheduled += self._schedule_batch(requests)
                    processed += len(requests)
                    self.logger.info("Processed %d, scheduled %d urls.", processed, scheduled)
                    requests = []
            except Exception:
                self.logger.exception("Error during seeds addition")
        if requests:
            try:
                scheduled += self._schedule_batch(requests)
            except Exception:
                self.logger.exception("Error during seeds addition")
            processed += len(requests)
        self.logger.info("Processed %d, and scheduled %d urls overall.", processed, scheduled)

    def _schedule_batch(self, requests):
        self.refresh_states(requests)
        scheduled = self.process_seeds(requests)
        self._states_ctx.release()
        return scheduled

    def process_seeds(self, seeds):
        """Handle and schedule a batch with seeds urls.

        We call seeds only those URLs which were injected during the crawling
        bootstrapping process. So seeds cannot be found during the crawling.
        """
        robots_requests = set()
        scheduled = 0
        for seed in seeds:
            parsed_url = urlsplit(seed.url)
            robots_url = "{url.scheme}://{url.netloc}/robots.txt".format(url=parsed_url)
            meta = {b'netloc': parsed_url.netloc,
                    b'seed': seed.url,
                    b'robots': True}
            request = self.create_request(robots_url, meta=meta, headers=DEFAULT_HEADERS)
            robots_requests.add(request)
        self.refresh_states(robots_requests)
        for request in robots_requests:
            if self._schedule_once(request, None, score=0.9):
                scheduled += 1
            else:
                self.logger.warning("The seed %s was already scheduled", request.url)
        return scheduled

    # Strategy main handlers section.

    def page_crawled(self, response):
        response.meta[b'state'] = States.CRAWLED
        # if redirects, response.url always contains initial url
        self.logger.debug("PC %s [%d] (seed: %s)", response.url,
                          response.status_code, response.meta.get(b'seed'))
        self._log_redirects_if_defined(response.request)
        is_succeeded = response.status_code in [200, 203, 206]
        netloc, _, domain = self._get_domain_after_redirects(response.request)
        if b'robots' in response.meta:
            if is_succeeded:
                self._process_robots_txt(response, domain)
            else:
                self._process_robots_txt_error(netloc, response.url, domain)
        elif b'sitemap' in response.meta:
            if is_succeeded:
                self._process_sitemap(netloc, response.body, domain)
            if is_accessible_domain(domain):
                self._schedule_home_page(netloc, domain)

    def filter_extracted_links(self, request, links):
        netloc, level_2nd_name, domain = self._get_domain_after_redirects(request)
        if is_domain_to_ignore(domain, max_pages=self.max_pages):
            return []
        robotparser = domain.get('_rp')
        chosen_links = []
        for link in links:
            if not self._is_from_same_domain(level_2nd_name, link):
                continue
            # validate that robots.txt allows to parse it (if defined)
            if robotparser and not robotparser.can_fetch(self.user_agent, link.url):
                continue
            chosen_links.append(link)
            # maybe ban the domain if it's eligible for ban
            link_netloc = urlsplit(link.url).netloc
            link_hostname, _, _ = link_netloc.partition(':')
            link_2nd_level, link_domain = self._get_domain(link_netloc)
            subdomains = link_domain.setdefault('subdomains', set())
            subdomains.add(link_hostname)
        return chosen_links

    def links_extracted(self, request, links):
        # if redirects, request.url contains final url
        self.logger.debug('LE %s (seed %s) %d extracted links',
                          request.url, request.meta.get(b'seed'), len(links))
        self._log_redirects_if_defined(request)
        _, level_2nd_name, domain = self._get_domain_after_redirects(request)
        for link in links:
            link.headers.update(DEFAULT_HEADERS)
        self._process_links(links, domain)

    def request_error(self, request, error):
        request.meta[b'state'] = States.ERROR
        # if redirects, request.url always contains initial url
        self.logger.debug("PE %s error: %s (seed: %s)",
                          request.url, error, request.meta.get(b'seed'))
        self._log_redirects_if_defined(request)
        netloc, _, domain = self._get_domain_after_redirects(request)
        if error == 'DNSLookupError':
            # marking DNS lookup error as fatal, to continue without discovery
            domain['fatal_error'] = error
        if b'robots' in request.meta:
            self._process_robots_txt_error(netloc, request.url, domain)
        elif b'sitemap' in request.meta and is_accessible_domain(domain):
            self._schedule_home_page(netloc, domain)

    # Additional helper handlers for robots.txt and sitemap logic.

    def _process_robots_txt(self, response, domain):
        """Handle robots.txt successful response.

        The main logic behind the method is to create a RobotFileParser instance
        if it's possible to decode and read robots.txt content, and save it as a
        property of domain to reuse it later when deciding about need to schedule
        a domain page or not.
        """
        netloc = response.meta[b'netloc']
        domain.setdefault('queued_pages', 0)
        try:
            body = response.body.decode('utf-8')  # TODO: use encoding from response.meta.get(b'encoding', 'utf-8')
        except UnicodeDecodeError:
            self.logger.warning("Error during robots.txt decoding at %s", response.url)
            update_domain_with_parser_data(domain, parser=None, url=response.url)
            self._schedule_home_page(netloc, domain)
            return
        robots_lines = body.splitlines()
        parser = robotparser.RobotFileParser(response.url)
        try:
            if not is_valid_robotstxt(robots_lines):
                raise SyntaxError("Robots.txt isn't valid")
            parser.parse(robots_lines)
        except Exception:
            self.logger.exception("Error during robots.txt parsing at %s", response.url)
            update_domain_with_parser_data(domain, parser=None, url=response.url)
            self._schedule_home_page(netloc, domain)
            return
        requests = set()
        for line in robots_lines:
            if line.startswith("Sitemap:"):
                _, _, url = line.partition(':')
                sitemap_url = urljoin(response.url, url.strip())
                meta = {b'seed': domain.get('seed'), b'sitemap': True,
                        b'scrapy_meta': {b'download_maxsize': SITEMAP_DOWNLOAD_MAXSIZE}}
                requests.add(self.create_request(sitemap_url, meta=meta, headers=DEFAULT_HEADERS))
        self.refresh_states(requests)
        # schedule sitemap requests
        self._schedule_requests(requests, domain, score=0.9)
        if not requests:
            self.logger.debug("Sitemap in robots.txt wasn't found for url %s", response.url)
        update_domain_with_parser_data(domain, parser=parser, url=response.url, body=body)
        # also always schedule home page regardless of scheduled sitemaps
        self._schedule_home_page(netloc, domain)

    def _process_robots_txt_error(self, netloc, url, domain):
        """Handle robots.txt failure response."""
        update_domain_with_parser_data(domain, parser=None, url=url)
        if is_accessible_domain(domain):
            self._schedule_home_page(netloc, domain)

    def _process_sitemap(self, netloc, body, domain):
        """Helper to process a sitemap request's response.

        Current logic is to split sitemap body content into sub-sitemaps and other
        entries, and schedule it (sub-sitemaps could be scheduled as-is with higher score,
        but other links should be processed differently exactly as links extracted from
        crawled page - sub-domains homepages have more priority over others requests).
        """
        if is_domain_to_ignore(domain, self.max_pages):
            return

        requests, sitemaps = set(), set()
        sitemap_scrapy_meta = {b'download_maxsize': SITEMAP_DOWNLOAD_MAXSIZE}
        for url, sub_sitemap in parse_sitemap(body):
            try:
                meta = {b'seed': domain.get('seed'), b'sitemap': True,
                        b'scrapy_meta': sitemap_scrapy_meta} if sub_sitemap else (
                            {b'home': True} if is_home_page_url(url) else {})
                request = self.create_request(url, meta=meta, headers=DEFAULT_HEADERS)
            except Exception:
                self.logger.exception("Error on url %s", url)
                continue
            sitemaps.add(request) if sub_sitemap else requests.add(request)
        # 1) handle sub-sitemaps
        if len(sitemaps) > MAX_SITEMAPS:
            # TODO global per-host counter of sitemaps scheduled
            self.logger.warning('Amount of sub-sitemaps > %d for url %s', MAX_SITEMAPS, netloc)
            sitemaps = set(random.sample(sitemaps, MAX_SITEMAPS))
        self.refresh_states(sitemaps)
        self._schedule_requests(sitemaps, domain, score=0.9, count=False)

        # 2) handle separate entries
        # current policy is to trust sitemap data, and don't verify hostname for links
        to_sample = self.max_pages - domain.get('queued_pages', 0)
        if to_sample > 0 and len(requests) > to_sample:
            requests = random.sample(requests, to_sample)
        self.refresh_states(requests)
        self._process_links(requests, domain)

    def _process_links(self, links, domain):
        """Helper to process and schedule extracted links.

        The method splits a given links set into 3 parts:
        - home pages for domain/sub-domain to schedule with higher score
        - links of interest
        - other pages
        (which is a string with domain name to check for inclusion).
        After splitting, the method schedules the requests.
        """
        if is_domain_to_ignore(domain, self.max_pages):
            return
        # at first schedule home pages with higher priority, and add others to a set
        home_links, interest_links, other_links = set(), set(), set()
        for link in links:
            link.meta[b'seed'] = domain.get('seed')
            if is_home_page_url(link.url):
                # XXX it may look proper to tag such links with meta[b'home'] = True,
                # but it would mean trusting to any home link found among extracted,
                # and lead to infinite amount of domains to crawl and infinite crawl
                home_links.add(link)
            elif self.is_link_of_interest(link):
                interest_links.add(link)
            else:
                other_links.add(link)
        self._schedule_requests(home_links, domain, score=0.8)
        self._schedule_requests(interest_links, domain, score=0.7)
        self._schedule_requests(other_links, domain, score=0.5)

    def is_link_of_interest(self, link):
        """Predicate helper to match important links.
        To be implemented in a subclass."""

    # Helpers to schedule different types of requests

    # The following 2 methods accept a dict with domain metadata and control amount
    # of queued pages already scheduled for the domain, please schedule all needed
    # requests only via the methods. Domain metadata also must contain seed field
    # to track it when validating results.

    def _schedule_home_page(self, netloc, domain):
        """Schedule a domain home page.

        The method enforces setting 'seed' meta field for the request.
        """
        if domain.setdefault('queued_pages', 0) >= self.max_pages:
            return
        home_page = "http://%s/" % netloc
        meta = {b'seed': domain.get('seed'), b'home': True}
        request = self.create_request(home_page, meta=meta, headers=DEFAULT_HEADERS)
        self.refresh_states([request])
        if self._schedule_once(request, domain, score=0.8):
            domain['queued_pages'] += 1
            self.logger.debug("Scheduled home page %s", request.url)
            return True
        return False

    def _schedule_requests(self, requests, domain, score, count=True):
        """Helper to schedule a bunch of requests in random order.

        The method schedules requests as-is w/o any modifications (except for score),
        make sure you have set all needed headers/metadata/etc before calling it.
        """
        scheduled = 0
        if not is_accessible_domain(domain):
            return scheduled
        already_queued_pages = domain.setdefault('queued_pages', 0)
        # XXX to avoid converting links set to a list if enough pages
        if count and already_queued_pages >= self.max_pages:
            return scheduled
        for request in consume_randomly(requests):
            # scheduling pages randomly if they fit within limits
            if count and domain['queued_pages'] >= self.max_pages:
                self.logger.debug('LIMIT REACHED pages (%d) for seed %s',
                                  domain['queued_pages'], domain['seed'])
                break
            if self._schedule_once(request, domain, score=score):
                self.logger.debug('IL Scheduled %s', request.url)
                domain['queued_pages'] += 1
                scheduled += 1
        return scheduled

    def _schedule_once(self, request, domain, score=0.1):
        """Accept a request object, justify its score and schedule it.

        The method schedules a request as-is w/o any modifications (except for score),
        make sure you have set all needed headers/metadata/etc before calling it.
        """
        robotparser = domain.get('_rp') if domain is not None else None
        if robotparser and not robotparser.can_fetch(self.user_agent, request.url):
            return False
        if request.meta[b'state'] != States.NOT_CRAWLED:
            return False
        hostname = urlsplit(request.url).hostname  # hostname is already lower-cased
        if not hostname:
            self.logger.warning("Can't parse hostname for '%s'", repr(request.url))
            return False
        final_score = justify_request_score_by_hostname(hostname, score)
        self.schedule(request, final_score)
        request.meta[b'state'] = States.QUEUED
        return True

    # Auxiliary helpers section

    def _get_domain_after_redirects(self, request):
        seed = request.meta.get(b'seed')
        redirect_urls = request.meta.get(b'redirect_urls', [])
        origin_url = redirect_urls[0] if redirect_urls else request.url
        origin_netloc = urlsplit(origin_url).netloc
        origin_2nd_name, origin_domain = self._get_domain(origin_netloc)

        if redirect_urls and (b'robots' in request.meta or
                              b'sitemap' in request.meta or
                              b'home' in request.meta):
            final_netloc = urlsplit(redirect_urls[-1]).netloc
            if final_netloc != origin_netloc:
                origin_redirects = origin_domain.setdefault('redirect_to', set())
                self._extend_redirects_list(origin_redirects, final_netloc)
                final_2nd_name, final_domain = self._get_domain(final_netloc)
                final_redirects = final_domain.setdefault('redirect_from', set())
                self._extend_redirects_list(final_redirects, origin_netloc)
                final_domain['seed'] = seed
                return final_netloc, final_2nd_name, final_domain

        origin_domain['seed'] = seed
        return origin_netloc, origin_2nd_name, origin_domain

    def _log_redirects_if_defined(self, request):
        redirect_urls = request.meta.get(b'redirect_urls', [])
        for redirect_url in redirect_urls:
            self.logger.debug("REDIR %s", redirect_url)

    def _extend_redirects_list(self, redirects, netloc):
        """Helper to add a netloc to redirects list within limit."""
        if netloc not in redirects and len(redirects) < MAX_DOMAINS_REDIRECTS_STORE:
            redirects.add(netloc)

    def _get_domain(self, netloc):
        """Helper to get a 2nd level domain and corresponding meta for a given netloc.
        Returns a tuple with a domain name and a metadata dict from domain cache.
        """
        domain = self._get_2ndlevel_name(netloc)
        return domain, self.domain_cache.setdefault(domain, {})

    def _is_from_same_domain(self, domain_name, request):
        """Helper to check if a request url points to the same domain."""
        return self._get_2ndlevel_name(urlsplit(request.url).netloc) == domain_name

    def _get_2ndlevel_name(self, netloc):
        """Helper to extract a host from netloc and get its public suffix."""
        hostname, _, _ = netloc.partition(':')
        return self._suffix_list.get_public_suffix(hostname)
