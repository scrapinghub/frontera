import pprint

from scrapy.core.scheduler import Scheduler
from scrapy.http import Request
from scrapy import log

from crawlfrontier import graphs

# Default Values
DEFAULT_RECORDER_ENABLED = True
DEFAULT_RECORDER_STORAGE_DROP_ALL_TABLES = True
DEFAULT_RECORDER_STORAGE_CLEAR_CONTENT = True

STATS_PREFIX = 'recorder'


class StatsManager(object):
    """
        'recorder/pages_count': xx,
        'recorder/seeds_count': xx,
        'recorder/links_count': xx,
    """
    def __init__(self, stats, prefix=STATS_PREFIX):
        self.stats = stats
        self.prefix = prefix

    def add_page(self, is_seed=False):
        self._inc_value('pages_count')
        if is_seed:
            self._inc_value('seeds_count')

    def remove_pages(self, count):
        self._inc_value('pages_count', -count)

    def add_link(self):
        self._inc_value('links_count')

    def remove_links(self, count):
        self._inc_value('links_count', -count)

    def _get_stats_name(self, variable):
        return '%s/%s' % (self.prefix, variable)

    def _inc_value(self, variable, count=1):
        self.stats.inc_value(self._get_stats_name(variable), count)

    def _set_value(self, variable, value):
        self.stats.set_value(self._get_stats_name(variable), value)


class RecorderScheduler(Scheduler):

    def open(self, spider):
        super(RecorderScheduler, self).open(spider)

        log.msg('Starting recorder', log.INFO)

        self.stats_manager = StatsManager(spider.crawler.stats)

        settings = spider.crawler.settings
        self.recorder_enabled = settings.get('RECORDER_ENABLED', DEFAULT_RECORDER_ENABLED)

        if not self.recorder_enabled:
            log.msg('Recorder disabled!', log.WARNING)
            return

        recorder_storage = settings.get('RECORDER_STORAGE_ENGINE', None)
        if not recorder_storage:
            self.recorder_enabled = False
            log.msg('Missing Recorder storage! Recorder disabled...', log.WARNING)
            return

        self.graph = graphs.Manager(
            engine=recorder_storage,
            drop_all_tables=settings.getbool('RECORDER_STORAGE_DROP_ALL_TABLES',
                                             DEFAULT_RECORDER_STORAGE_DROP_ALL_TABLES),
            clear_content=settings.getbool('RECORDER_STORAGE_CLEAR_CONTENT',
                                           DEFAULT_RECORDER_STORAGE_CLEAR_CONTENT))

    def close(self, reason):
        super(RecorderScheduler, self).close(reason)
        log.msg('Finishing recorder (%s)' % reason, log.INFO)
        pages = self.graph.session.query(graphs.Page).filter_by(status=None).all()
        for page in pages:
            n_deleted_links = self.graph.session.query(graphs.Relation).filter_by(child_id=page.id).delete()
            if n_deleted_links:
                self.stats_manager.remove_links(n_deleted_links)
        n_deleted_pages = self.graph.session.query(graphs.Page).filter_by(status=None).delete()
        if n_deleted_pages:
            self.stats_manager.remove_pages(n_deleted_pages)
        self.graph.save()

    def enqueue_request(self, request):
        enqueued = super(RecorderScheduler, self).enqueue_request(request)
        if self.recorder_enabled and enqueued:
            is_seed = 'rule' not in request.meta and \
                      'origin_is_recorder' not in request.meta
            page = self.graph.add_page(url=request.url, is_seed=is_seed)
            self.stats_manager.add_page(is_seed)
            request.meta['is_seed'] = is_seed
            request.meta['page'] = page
        return enqueued

    def next_request(self):
        request = super(RecorderScheduler, self).next_request()
        if request:
            request.meta['origin_is_recorder'] = True
        return request

    def process_spider_output(self, result, request, response, spider):
        if not self.recorder_enabled:
            for r in result:
                yield r

        page = response.meta['page']
        page.status = response.status
        self.graph.save()
        requests = [r for r in result if isinstance(r, Request)]
        for request in requests:
            link = self.graph.add_link(page=page, url=request.url)
            request.meta['page'] = link
            request.meta['referer'] = page
            self.stats_manager.add_link()
            yield request

    def process_download_error(self, spider_failure, download_failure, request, spider):
        error_code = self._get_failure_code(download_failure or spider_failure)
        page = request.meta['page']
        page.status = error_code
        self.graph.save()

    def _get_failure_code(self, failure):
        try:
            return failure.type.__name__
        except:
            return '?'
