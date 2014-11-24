from twisted.internet.error import DNSLookupError, TimeoutError
from twisted.internet.task import LoopingCall
from scrapy.exceptions import NotConfigured, DontCloseSpider
from scrapy.http import Request
from scrapy import signals

from crawlfrontier import FrontierManager

# Signals
frontier_download_error = object()

# Defaul values
DEFAULT_FRONTIER_ENABLED = True
DEFAULT_FRONTIER_SCHEDULER_INTERVAL = 0.5
DEFAULT_FRONTIER_SCHEDULER_CONCURRENT_REQUESTS = 256


class CrawlFrontierSpiderMiddleware(object):

    def __init__(self, crawler, stats):
        self.crawler = crawler
        self.stats = stats

        # Enable check
        if not crawler.settings.get('FRONTIER_ENABLED', DEFAULT_FRONTIER_ENABLED):
            raise NotConfigured

        # Frontier
        frontier_settings = crawler.settings.get('FRONTIER_SETTINGS', None)
        if not frontier_settings:
            raise NotConfigured
        self.frontier = FrontierManager.from_settings(frontier_settings)

        # Scheduler settings
        self.scheduler_interval = crawler.settings.get('FRONTIER_SCHEDULER_INTERVAL',
                                                       DEFAULT_FRONTIER_SCHEDULER_INTERVAL)
        self.scheduler_concurrent_requests = crawler.settings.get('FRONTIER_SCHEDULER_CONCURRENT_REQUESTS',
                                                                  DEFAULT_FRONTIER_SCHEDULER_CONCURRENT_REQUESTS)
        # Queued pages set
        self.queued_pages = set()

        # Signals
        self.crawler.signals.connect(self.spider_opened, signals.spider_opened)
        self.crawler.signals.connect(self.spider_closed, signals.spider_closed)
        #self.crawler.signals.connect(self.response_received, signals.response_received)
        self.crawler.signals.connect(self.spider_idle, signals.spider_idle)
        self.crawler.signals.connect(self.download_error, frontier_download_error)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler, crawler.stats)

    def spider_opened(self, spider):
        if not self.frontier.auto_start:
            self.frontier.start()
        self.next_requests_task = LoopingCall(self._schedule_next_requests, spider)
        self.next_requests_task.start(self.scheduler_interval)

    def spider_closed(self, spider, reason):
        self.next_requests_task.stop()
        self.frontier.stop()

    def process_start_requests(self, start_requests, spider):
        if start_requests:
            self.frontier.logger.debugging.warning("gets seeds")
            self.frontier.add_seeds([req.url for req in start_requests])
        self.frontier.logger.debugging.warning("added seeds")
        return self._get_next_requests()

    def process_spider_output(self, response, result, spider):
        page = response.meta['page']
        page.status = response.status
        requests = []
        for element in result:
            if isinstance(element, Request):
                requests.append(element)
            else:
                yield element
        self.frontier.logger.debugging.warning('spider output: %s' % requests)
        self.frontier.page_crawled(page=page,
                                   links=[r.url for r in requests])
        self._remove_queued_page(page)

    def download_error(self, request, exception, spider):
        # TO-DO: Add more errors...
        error = '?'
        if isinstance(exception, DNSLookupError):
            error = 'DNS_ERROR'
        elif isinstance(exception, TimeoutError):
            error = 'TIMEOUT_ERROR'

        page = request.meta['page']
        self.frontier.page_crawled_error(page, error)
        self._remove_queued_page(page)

    def spider_idle(self, spider):
        self.frontier.logger.debugging.warning('spider_idle')
        if not self.frontier.finished:
            raise DontCloseSpider()

    def _schedule_next_requests(self, spider):
        n_scheduled = len(self.queued_pages)
        if not self.frontier.finished and n_scheduled < self.scheduler_concurrent_requests:
            n_requests_gap = self.scheduler_concurrent_requests - n_scheduled
            next_pages = self._get_next_requests(n_requests_gap)
            for request in next_pages:
                self.crawler.engine.crawl(request, spider)

    def _get_next_requests(self, max_next_pages=0):
        requests = []
        for page in self.frontier.get_next_pages(max_next_pages=max_next_pages):
            request = Request(url=page.url, dont_filter=True)
            request.meta['page'] = page
            requests.append(request)
            self._add_queued_page(page)
        return requests

    def _add_queued_page(self, page):
        self.queued_pages.add(page.fingerprint)

    def _remove_queued_page(self, page):
        self.queued_pages.remove(page.fingerprint)


class CrawlFrontierDownloaderMiddleware(object):
    def __init__(self, crawler):
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_exception(self, request, exception, spider):
        self.crawler.signals.send_catch_log(signal=frontier_download_error,
                                            request=request,
                                            exception=exception,
                                            spider=spider)

