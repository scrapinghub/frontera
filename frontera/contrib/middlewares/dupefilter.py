from frontera.core.components import Middleware
from frontera.utils.misc import load_object

class BasicDupeFilter(object):
    def __init__(self):
        self.seen = set()

    def request_seen(self, request):
        fp = request.meta['fingerprint']
        if fp in self.seen:
            return True
        self.seen.add(fp)
        return False

    @classmethod
    def from_manager(cls, manager):
        return cls()

DEFAULT_DUPE_FILTER = 'frontera.contrib.middlewares.dupefilter.BasicDupeFilter'
class DupeFilterMiddleware(Middleware):
    component_name = 'DupeFilter Middleware'

    def __init__(self, manager):
        self.manager = manager
        dpfclassname = manager.settings.get('DUPEFILTER_CLASS', DEFAULT_DUPE_FILTER)
        dpfclass = load_object(dpfclassname)
        self.dupefilter = dpfclass.from_manager(manager)

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass

    def add_seeds(self, seeds):
        return seeds

    def page_crawled(self, response, links):
        return response

    def process_request(self, request):
        if self.dupefilter.request_seen(request) and not request.meta.get('dont_filter', False):
            self.manager.logger.debugging.debug('<Filtered already seen request <{}>'.format(request.url))
        else:
            return request

    def request_error(self, request, error):
        pass
