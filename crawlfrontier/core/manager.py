from collections import OrderedDict

from crawlfrontier.exceptions import NotConfigured
from crawlfrontier.utils.misc import load_object
from crawlfrontier.settings import Settings
from crawlfrontier.core.components import Backend, Middleware
from crawlfrontier.logger import FrontierLogger
from models import Page, Link


class FrontierManager(object):

    def __init__(self, page_model, link_model, backend, logger, event_log_manager,
                 frontier_middlewares=None, test_mode=False,
                 max_pages=0, max_next_pages=0, auto_start=True, settings=None):

        # Settings
        self._settings = settings or Settings()

        # Logger
        self._logger = load_object(logger)(self._settings)
        assert isinstance(self._logger, FrontierLogger), "logger '%s' must subclass FrontierLogger" % \
                                                         self._logger.__class__.__name__

        # Log frontier manager starting
        self.logger.manager.debug('-'*80)
        self.logger.manager.debug('Starting Frontier Manager...')

        # Test mode
        self._test_mode = test_mode
        self.logger.manager.debug('Test mode %s' % ("ENABLED" if self.test_mode else "DISABLED"))

        # Load page model
        self._page_model = load_object(page_model)
        assert issubclass(self._page_model, Page), "Page model '%s' must subclass Page" % \
                                                   self._page_model.__name__

        # Load link model
        self._link_model = load_object(link_model)
        assert issubclass(self._link_model, Link), "Page model '%s' must subclass Link" % \
                                                   self._link_model.__name__

        # Load middlewares
        self._frontier_middlewares = self._load_middlewares(frontier_middlewares)

        # Load backend
        self.logger.manager.debug("Loading backend '%s'" % backend)
        self._backend = self._load_object(backend)
        assert isinstance(self.backend, Backend), "backend '%s' must subclass Backend" % \
                                                  self.backend.__class__.__name__

        # Init frontier components pipeline
        self._components_pipeline = [
            ('Middleware', self.frontier_middlewares),
            ('Backend', self.backend),
        ]

        # Page counters
        self._max_pages = max_pages
        self._max_next_pages = max_next_pages
        self._n_pages = 0

        # Iteration counter
        self._iteration = 0

        # Manager finished flag
        self._finished = False

        # Load Event log manager
        self.logger.manager.debug("Loading event log manager '%s'" % event_log_manager)
        self._event_log_manager = self._load_object(event_log_manager)

        # Log frontier manager start
        self.logger.manager.debug('Frontier Manager Started!')
        self.logger.manager.debug('-'*80)

        # start/stop
        self._started = False
        self._stopped = False
        self._auto_start = auto_start
        if self.auto_start:
            self.start()


    @classmethod
    def from_settings(cls, settings=None):
        manager_settings = Settings(settings)
        return FrontierManager(page_model=manager_settings.PAGE_MODEL,
                               link_model=manager_settings.LINK_MODEL,
                               backend=manager_settings.BACKEND,
                               logger=manager_settings.LOGGER,
                               event_log_manager=manager_settings.EVENT_LOG_MANAGER,
                               frontier_middlewares=manager_settings.MIDDLEWARES,
                               test_mode=manager_settings.TEST_MODE,
                               max_pages=manager_settings.MAX_PAGES,
                               max_next_pages=manager_settings.MAX_NEXT_PAGES,
                               auto_start=manager_settings.AUTO_START,
                               settings=manager_settings)
    @property
    def settings(self):
        return self._settings

    @property
    def logger(self):
        return self._logger

    @property
    def test_mode(self):
        return self._test_mode

    @property
    def page_model(self):
        return self._page_model

    @property
    def link_model(self):
        return self._link_model

    @property
    def frontier_middlewares(self):
        return self._frontier_middlewares

    @property
    def backend(self):
        return self._backend

    @property
    def auto_start(self):
        return self._auto_start

    @property
    def event_log_manager(self):
        return self._event_log_manager

    @property
    def iteration(self):
        return self._iteration

    @property
    def max_pages(self):
        return self._max_pages

    @property
    def max_next_pages(self):
        return self._max_next_pages

    @property
    def n_pages(self):
        return self._n_pages

    def start(self):
        assert not self._started, 'Frontier already started!'
        self.event_log_manager.frontier_start()
        self.logger.manager.debug(self._msg('START'))
        self._process_components(method_name='frontier_start')
        self._started = True

    def stop(self):
        self._check_startstop()
        self.logger.manager.debug(self._msg('STOP'))
        self._process_components(method_name='frontier_stop')
        self._stopped = True
        self.event_log_manager.frontier_stop()

    def add_seeds(self, urls):
        self._check_startstop()
        links = [self._link_model(url=url) for url in urls]
        self.event_log_manager.add_seeds(links)
        self.logger.manager.debug(self._msg('ADD_SEEDS urls_length=%s' % len(urls)))
        return self._process_components(method_name='add_seeds',
                                        obj=links,
                                        return_classes=(list,))

    def page_crawled(self, page, links=None):
        self._check_startstop()
        self.logger.manager.debug(self._msg('PAGE_CRAWLED url=%s status=%s links=%s'
                                            % (page.url, page.status, len(links) if links else 0)))
        processed_page = self._process_components(method_name='page_crawled',
                                                  obj=page,
                                                  return_classes=Page,
                                                  links=[self._link_model(url=url) for url in links] if links else [])
        self.event_log_manager.page_crawled(processed_page, links)
        return processed_page

    def page_crawled_error(self, page, error):
        self._check_startstop()
        self.logger.manager.debug(self._msg('PAGE_CRAWLED_ERROR url=%s' % page.url))
        processed_page = self._process_components(method_name='page_crawled_error',
                                                  obj=page,
                                                  return_classes=Page,
                                                  error=error)
        self.event_log_manager.page_crawled_error(processed_page, error)
        return processed_page

    def get_next_pages(self, max_next_pages=0):
        self._check_startstop()

        # End condition check
        if self.max_pages and self.n_pages >= self.max_pages:
            self.logger.manager.warning(self._msg('MAX PAGES REACHED! (%s/%s)' % (self.n_pages, self.max_pages)))
            self.finished = True
            return []

        # Calculate number of pages to request
        max_next_pages = max_next_pages or self.max_next_pages
        if self.max_pages:
            if not max_next_pages:
                max_next_pages = self.max_pages - self.n_pages
            else:
                if self.n_pages+max_next_pages > self.max_pages:
                    max_next_pages = self.max_pages - self.n_pages

        # log (in)
        self.logger.manager.debug(self._msg('GET_NEXT_PAGES(in) max_next_pages=%s n_pages=%s/%s' %
                                            (max_next_pages, self.n_pages, self.max_pages or '-')))

        # Request next pages
        next_pages = self.backend.get_next_pages(max_next_pages)

        # Increment page counter
        self._n_pages += len(next_pages)

        # Increment Iteration and log event
        if next_pages:
            self._iteration += 1
            self.event_log_manager.get_next_pages(max_next_pages, next_pages)

        # log (out)
        self.logger.manager.debug(self._msg('GET_NEXT_PAGES(out) returned_pages=%s n_pages=%s/%s' %
                                            (len(next_pages), self.n_pages, self.max_pages or '-')))

        # Return next pages
        return next_pages

    def get_page(self, url):
        self._check_startstop()
        self.logger.manager.debug(self._msg('GET_PAGE url=%s' % url))
        return self._process_components(method_name='get_page',
                                        obj=self._link_model(url=url),
                                        return_classes=Page)

    def _msg(self, msg):
        return '(%s) %s' % (self.iteration, msg)

    def _load_object(self, obj_class_name, silent=False):
        obj_class = load_object(obj_class_name)
        try:
            return self._load_frontier_object(obj_class)
        except NotConfigured:
            if not silent:
                raise NotConfigured

    def _load_frontier_object(self, obj_class):
        if hasattr(obj_class, 'from_manager'):
            return obj_class.from_manager(self)
        else:
            return obj_class()

    def _load_middlewares(self, middleware_names):
        # TO-DO: Use dict for middleware ordering
        mws = []
        for mw_name in middleware_names or []:
            self.logger.manager.debug("Loading middleware '%s'" % mw_name)
            mw = self._load_object(mw_name, silent=True)
            assert isinstance(mw, Middleware), "middleware '%s' must subclass Middleware" % \
                                               mw.__class__.__name__
            if mw:
                mws.append(mw)
        return mws

    def _process_components(self, method_name, obj=None, return_classes=None, **kwargs):
        return_obj = obj
        for component_category, component in self._components_pipeline:
            components = component if isinstance(component, list) else [component]
            for component in components:
                return_obj = self._process_component(component=component, method_name=method_name,
                                                     component_category=component_category, obj=return_obj,
                                                     return_classes=return_classes,
                                                     **kwargs)
                if obj and not return_obj:
                    self.logger.manager.warning("Object '%s' filtered in '%s' by '%s'" % (
                        obj.__class__.__name__, method_name, component.__class__.__name__
                    ))
                    return
        return return_obj

    def _process_component(self, component, method_name, component_category, obj, return_classes, **kwargs):
        debug_msg = "processing '%s' '%s.%s' %s" % (method_name, component_category, component.__class__.__name__, obj)
        self.logger.debugging.debug(debug_msg)
        return_obj = getattr(component, method_name)(*([obj] if obj else []), **kwargs)
        assert return_obj is None or isinstance(return_obj, return_classes), \
            "%s '%s.%s' must return None or %s, Got '%s'" % \
            (component_category, obj.__class__.__name__, method_name,
             ' or '.join(c.__name__ for c in return_classes)
             if isinstance(return_classes, tuple) else
             return_classes.__name__,
             return_obj.__class__.__name__)
        return return_obj

    def _check_startstop(self):
        assert self._started, "Frontier not started!"
        assert not self._stopped, "Call to stopped frontier!"

    def _log_event(self, event, params=None):
        event_params = OrderedDict()
        event_params['job_id'] = self.job_id
        event_params['iteration'] = self.iteration
        if params:
            event_params.update(params)
        self.logger.events.event(event=event, params=event_params)
