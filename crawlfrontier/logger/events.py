import os
from collections import OrderedDict


class EventLogManager(object):

    def __init__(self, manager):
        self.manager = manager
        self.job_id = os.environ.get('FRONTIER_JOB_ID', None)
        self.include_metadata = manager.settings.get('LOGGING_EVENTS_INCLUDE_METADATA')
        self.include_domain = manager.settings.get('LOGGING_EVENTS_INCLUDE_DOMAIN')
        self.include_domain_fields = manager.settings.get('LOGGING_EVENTS_INCLUDE_DOMAIN_FIELDS')

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def frontier_start(self):
        self._log(event='FRONTIER_START')

    def frontier_stop(self):
        self._log(event='FRONTIER_STOP')

    def add_seed(self, link):
        params = OrderedDict()
        self._add_url_info(params, link)
        self._log(event='ADD_SEED', params=params)

    def add_seeds(self, links):
        params = OrderedDict()
        params['n_seeds'] = len(links)
        self._log(event='ADD_SEEDS', params=params)

    def page_crawled(self, page, links):
        params = OrderedDict()
        self._add_url_info(params, page)
        params['n_links'] = len(links) if links else 0
        params['http_code'] = page.status
        if self.include_metadata:
            for meta_name, meta_value in page.meta.iteritems():
                params['metadata_'+meta_name] = meta_value
        self._log(event='PAGE_CRAWLED', params=params)

    def page_crawled_error(self, page, error):
        params = OrderedDict()
        self._add_url_info(params, page)
        params['error'] = error
        self._log(event='PAGE_CRAWLED_ERROR', params=params)

    def get_next_pages(self, max_next_pages, next_pages):
        if next_pages:
            params = OrderedDict()
            params['n_next_pages'] = len(next_pages)
            params['max_next_pages'] = max_next_pages
            self._log(event='GET_NEXT_PAGES', params=params)

    def _log(self, event, params=None):
        event_params = OrderedDict()
        event_params['job_id'] = self.job_id
        event_params['iteration'] = self.manager.iteration
        if params:
            event_params.update(params)
        self.manager.logger.events.event(event=event, params=event_params)

    def _add_url_info(self, params, obj):
        params['url'] = obj.url
        if self.include_domain and hasattr(obj, 'domain'):
            for field in self.include_domain_fields:
                params['domain_'+field] = getattr(obj.domain, field)
