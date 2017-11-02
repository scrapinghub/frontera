from __future__ import absolute_import
import pprint

from scrapy.core.scheduler import Scheduler
from scrapy.http import Request
from scrapy import log

from frontera import graphs

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

        self.stats_manager = StatsManager(spider.crawler.stats)

        settings = spider.crawler.settings
        self.recorder_enabled = settings.get('RECORDER_ENABLED', DEFAULT_RECORDER_ENABLED)

        if not self.recorder_enabled:
            log.msg('Recorder disabled!', log.WARNING)
            return

        log.msg('Starting recorder', log.INFO)

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
        if self.recorder_enabled:
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
        if not request.dont_filter and self.df.request_seen(request):
            self.df.log(request, self.spider)
            return
        dqok = self._dqpush(request)
        if dqok:
            self.stats.inc_value('scheduler/enqueued/disk', spider=self.spider)
        else:
            self._mqpush(request)
            self.stats.inc_value('scheduler/enqueued/memory', spider=self.spider)
        self.stats.inc_value('scheduler/enqueued', spider=self.spider)
        if self.recorder_enabled:
            is_seed = b'rule' not in request.meta and \
                      b'origin_is_recorder' not in request.meta
            page = self.graph.add_page(url=request.url, is_seed=is_seed)
            self.stats_manager.add_page(is_seed)
            request.meta[b'is_seed'] = is_seed
            request.meta[b'page'] = page

    def next_request(self):
        request = super(RecorderScheduler, self).next_request()
        if self.recorder_enabled and request:
            request.meta[b'origin_is_recorder'] = True
        return request

    def process_spider_output(self, response, result, spider):
        if not self.recorder_enabled:
            for r in result:
                yield r
            return

        page = response.meta[b'page']
        page.status = response.status
        self.graph.save()
        requests = [r for r in result if isinstance(r, Request)]
        for request in requests:
            link = self.graph.add_link(page=page, url=request.url)
            request.meta[b'page'] = link
            request.meta[b'referer'] = page
            self.stats_manager.add_link()
            yield request

    def process_exception(self, request, exception, spider):
        if self.recorder_enabled:
            error_code = self._get_exception_code(exception)
            page = request.meta[b'page']
            page.status = error_code
            self.graph.save()

    def _get_exception_code(self, exception):
        try:
            return exception.__class__.__name__
        except Exception:
            return '?'
