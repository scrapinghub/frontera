from scrapy.exceptions import NotConfigured
from scrapy.http import Request
from scrapy import signals

from crawlfrontier import graphs

recorder_download_error = object()

# Default Values
DEFAULT_RECORDER_ENABLED = True
DEFAULT_RECORDER_STORAGE_DROP_ALL_TABLES = True
DEFAULT_RECORDER_STORAGE_CLEAR_CONTENT = True


class CrawlRecorderSpiderMiddleware(object):
    def __init__(self, crawler, stats):
        self.crawler = crawler
        self.stats = stats

        settings = crawler.settings

        # Enable check
        if not settings.get('RECORDER_ENABLED', DEFAULT_RECORDER_ENABLED):
            raise NotConfigured

        recorder_storage = settings.get('RECORDER_STORAGE_ENGINE', None)
        if not recorder_storage:
            raise NotConfigured

        self.graph = graphs.Manager(engine=recorder_storage,
                                    drop_all_tables=settings.getbool('RECORDER_STORAGE_DROP_ALL_TABLES',
                                                                     DEFAULT_RECORDER_STORAGE_DROP_ALL_TABLES),
                                    clear_content=settings.getbool('RECORDER_STORAGE_CLEAR_CONTENT',
                                                                   DEFAULT_RECORDER_STORAGE_CLEAR_CONTENT))

        self.crawler.signals.connect(self.spider_closed, signals.spider_closed)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler, crawler.stats)

    def process_start_requests(self, start_requests, spider):
        for request in start_requests:
            page = self.graph.add_page(url=request.url, is_seed=True)
            request.meta['page'] = page
            yield request

    def process_spider_output(self, response, result, spider):
        page = response.meta['page']
        page.status = response.status
        self.graph.save()
        requests = [r for r in result if isinstance(r, Request)]
        for request in requests:
            link = self.graph.add_link(page=page, url=request.url)
            request.meta['page'] = link
            request.meta['referer'] = page
            yield request

    def spider_closed(self, spider, reason):
        pages = self.graph.session.query(graphs.Page).filter_by(status=None).all()
        for page in pages:
            self.graph.session.query(graphs.Relation).filter_by(child_id=page.id).delete()
        self.graph.session.query(graphs.Page).filter_by(status=None).delete()
        self.graph.save()

    def _get_url(self, response):
        return response.meta['redirect_urls'][0] if 'redirect_urls' in response.meta else response.request.url


class CrawlRecorderDownloaderMiddleware(object):
    def __init__(self, crawler):
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_exception(self, request, exception, spider):
        self.crawler.signals.send_catch_log(signal=recorder_download_error,
                                            request=request,
                                            exception=exception,
                                            spider=spider)

