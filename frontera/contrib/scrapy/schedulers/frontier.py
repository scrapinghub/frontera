from __future__ import absolute_import
from scrapy.core.scheduler import Scheduler
from scrapy.http import Request
from logging import getLogger

from collections import deque
from time import time

from frontera.contrib.scrapy.manager import ScrapyFrontierManager
from frontera.contrib.scrapy.settings_adapter import ScrapySettingsAdapter
import six

STATS_PREFIX = 'frontera'


class StatsManager(object):
    """
        'frontera/crawled_pages_count': 489,
        'frontera/crawled_pages_count/200': 382,
        'frontera/crawled_pages_count/301': 37,
        'frontera/crawled_pages_count/302': 58,
        'frontera/crawled_pages_count/400': 5,
        'frontera/crawled_pages_count/403': 1,
        'frontera/crawled_pages_count/404': 1,
        'frontera/crawled_pages_count/999': 5,
        'frontera/iterations': 5,
        'frontera/links_extracted_count': 39805,
        'frontera/pending_requests_count': 0,
        'frontera/redirected_requests_count': 273,
        'frontera/request_errors_count': 11,
        'frontera/request_errors_count/DNSLookupError': 1,
        'frontera/request_errors_count/ResponseNeverReceived': 9,
        'frontera/request_errors_count/TimeoutError': 1,
        'frontera/returned_requests_count': 500,
    """
    def __init__(self, stats, prefix=STATS_PREFIX):
        self.stats = stats
        self.prefix = prefix

    def add_seeds(self, count=1):
        self._inc_value('seeds_count', count)

    def add_crawled_page(self, status_code, n_links):
        self._inc_value('crawled_pages_count')
        self._inc_value('crawled_pages_count/%s' % str(status_code))
        self._inc_value('links_extracted_count', n_links)

    def add_redirected_requests(self, count=1):
        self._inc_value('redirected_requests_count', count)

    def add_returned_requests(self, count=1):
        self._inc_value('returned_requests_count', count)

    def add_request_error(self, error_code):
        self._inc_value('request_errors_count')
        self._inc_value('request_errors_count/%s' % str(error_code))

    def set_iterations(self, iterations):
        self._set_value('iterations', iterations)

    def set_pending_requests(self, pending_requests):
        self._set_value('pending_requests_count', pending_requests)

    def _get_stats_name(self, variable):
        return '%s/%s' % (self.prefix, variable)

    def _inc_value(self, variable, count=1):
        self.stats.inc_value(self._get_stats_name(variable), count)

    def _set_value(self, variable, value):
        self.stats.set_value(self._get_stats_name(variable), value)


class FronteraScheduler(Scheduler):

    def __init__(self, crawler, manager=None):
        self.crawler = crawler
        self.stats_manager = StatsManager(crawler.stats)
        self._pending_requests = deque()
        self.redirect_enabled = crawler.settings.get('REDIRECT_ENABLED')
        settings = ScrapySettingsAdapter(crawler.settings)
        self.frontier = ScrapyFrontierManager(settings, manager)
        self._delay_on_empty = self.frontier.manager.settings.get('DELAY_ON_EMPTY')
        self._delay_next_call = 0.0
        self.logger = getLogger('frontera.contrib.scrapy.schedulers.FronteraScheduler')

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def enqueue_request(self, request):
        if self.redirect_enabled:
            self._add_pending_request(request)
            self.stats_manager.add_redirected_requests()
            return True
        self.logger.warning("The enqueue_request failed on %s", request.url)
        return False

    def next_request(self):
        request = self._get_next_request()
        if request:
            self.stats_manager.add_returned_requests()
        return request

    def process_spider_output(self, response, result, spider):
        links = []
        for element in result:
            if isinstance(element, Request):
                links.append(element)
            else:
                yield element
        frontier_request = response.meta[b'frontier_request']
        self.frontier.page_crawled(response)  # removed frontier part from .meta
        # putting it back, to persist .meta from original request
        response.meta[b'frontier_request'] = frontier_request
        self.frontier.links_extracted(response.request, links)
        self.stats_manager.add_crawled_page(response.status, len(links))

    def process_exception(self, request, exception, spider):
        error_code = self._get_exception_code(exception)
        self.frontier.request_error(request=request, error=error_code)
        self.stats_manager.add_request_error(error_code)

    def open(self, spider):
        self.frontier.set_spider(spider)
        self.logger.info("Starting frontier")
        if not self.frontier.manager.auto_start:
            self.frontier.start()

    def close(self, reason):
        self.logger.info("Finishing frontier (%s)", reason)
        self.frontier.stop()
        self.stats_manager.set_iterations(getattr(self.frontier.manager, 'iteration', 0))
        self.stats_manager.set_pending_requests(len(self))

    def __len__(self):
        return len(self._pending_requests)

    def has_pending_requests(self):
        return not self.frontier.finished()

    def _get_next_request(self):
        if not self.frontier.manager.finished and \
                len(self) < self.crawler.engine.downloader.total_concurrency and \
                self._delay_next_call < time():

            info = self._get_downloader_info()
            requests = self.frontier.get_next_requests(key_type=info['key_type'], overused_keys=info['overused_keys'])
            for request in requests:
                self._add_pending_request(request)
            self._delay_next_call = time() + self._delay_on_empty if not requests else 0.0
        return self._get_pending_request()

    def _add_pending_request(self, request):
        return self._pending_requests.append(request)

    def _get_pending_request(self):
        return self._pending_requests.popleft() if self._pending_requests else None

    def _get_exception_code(self, exception):
        try:
            return exception.__class__.__name__
        except Exception:
            return '?'

    def _get_downloader_info(self):
        downloader = self.crawler.engine.downloader
        info = {
            'key_type': 'ip' if downloader.ip_concurrency else 'domain',
            'overused_keys': []
        }
        for key, slot in six.iteritems(downloader.slots):
            overused_factor = len(slot.active) / float(slot.concurrency)
            if overused_factor > self.frontier.manager.settings.get('OVERUSED_SLOT_FACTOR'):
                info['overused_keys'].append(key)
        return info
