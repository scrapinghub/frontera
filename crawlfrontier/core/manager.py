from collections import OrderedDict

from crawlfrontier.exceptions import NotConfigured
from crawlfrontier.utils.misc import load_object
from crawlfrontier.settings import Settings
from crawlfrontier.core.components import Backend, Middleware
from crawlfrontier.logger import FrontierLogger
from crawlfrontier.core import models


class FrontierManager(object):

    def __init__(self, request_model, response_model, backend, logger, event_log_manager,
                 frontier_middlewares=None, test_mode=False,
                 max_requests=0, max_next_requests=0, auto_start=True, settings=None):

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

        # Load request model
        self._request_model = load_object(request_model)
        assert issubclass(self._request_model, models.Request), "Request model '%s' must subclass 'Request'" % \
                                                                self._request_model.__name__

        # Load response model
        self._response_model = load_object(response_model)
        assert issubclass(self._response_model, models.Response), "Response model '%s' must subclass 'Response'" % \
                                                                  self._response_model.__name__

        # Load middlewares
        self._frontier_middlewares = self._load_middlewares(frontier_middlewares)

        # Load backend
        self.logger.manager.debug("Loading backend '%s'" % backend)
        self._backend = self._load_object(backend)
        assert isinstance(self.backend, Backend), "backend '%s' must subclass Backend" % \
                                                  self.backend.__class__.__name__

        # Init frontier components pipeline
        self._components_pipeline = [
            ('Middleware', self.frontier_middlewares, True),
            ('Backend', self.backend, False),
        ]

        # Page counters
        self._max_requests = max_requests
        self._max_next_requests = max_next_requests
        self._n_requests = 0

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
        return FrontierManager(request_model=manager_settings.REQUEST_MODEL,
                               response_model=manager_settings.RESPONSE_MODEL,
                               backend=manager_settings.BACKEND,
                               logger=manager_settings.LOGGER,
                               event_log_manager=manager_settings.EVENT_LOG_MANAGER,
                               frontier_middlewares=manager_settings.MIDDLEWARES,
                               test_mode=manager_settings.TEST_MODE,
                               max_requests=manager_settings.MAX_REQUESTS,
                               max_next_requests=manager_settings.MAX_NEXT_REQUESTS,
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
    def response_model(self):
        return self._response_model

    @property
    def request_model(self):
        return self._request_model

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
    def max_requests(self):
        return self._max_requests

    @property
    def max_next_requests(self):
        return self._max_next_requests

    @property
    def n_requests(self):
        return self._n_requests

    @property
    def finished(self):
        return self._finished

    def start(self):
        assert not self._started, 'Frontier already started!'
        #self.event_log_manager.frontier_start()
        self.logger.manager.debug(self._msg('START'))
        self._process_components(method_name='frontier_start')
        self._started = True

    def stop(self):
        self._check_startstop()
        self.logger.manager.debug(self._msg('STOP'))
        self._process_components(method_name='frontier_stop')
        self._stopped = True
        #self.event_log_manager.frontier_stop()

    def add_seeds(self, seeds):
        self._check_startstop()
        # FIXME probably seeds should be a generator here
        assert len(seeds), "Empty seeds list"
        for seed in seeds:
            assert isinstance(seed, self._request_model), "Seed objects must subclass '%s', '%s' found" % \
                                                          (self._request_model.__name__, type(seed).__name__)
        #self.event_log_manager.add_seeds(seeds)
        self.logger.manager.debug(self._msg('ADD_SEEDS urls_length=%s' % len(seeds)))
        self._process_components(method_name='add_seeds',
                                 obj=seeds,
                                 return_classes=(list,))  # TODO: Dar vuelta

    def page_crawled(self, response, links=None):
        self._check_startstop()
        self.logger.manager.debug(self._msg('PAGE_CRAWLED url=%s status=%s links=%s'%
                                            (response.url, response.status_code, len(links) if links else 0)))
        assert isinstance(response, self.response_model), "Response object must subclass '%s', '%s' found" % \
                                                          (self.response_model.__name__, type(response).__name__)
        assert hasattr(response, 'request') and response.request, "Empty response request"
        assert isinstance(response.request, self.request_model), "Response request object must subclass '%s', " \
                                                                 "'%s' found" % \
                                                                  (self.request_model.__name__,
                                                                  type(response.request).__name__)
        assert isinstance(response, self.response_model), "Response object must subclass '%s', '%s' found" % \
                                                          (self.response_model.__name__, type(response).__name__)
        if links:
            for link in links:
                assert isinstance(link, self._request_model), "Link objects must subclass '%s', '%s' found" % \
                                                              (self._request_model.__name__, type(link).__name__)
        self._process_components(method_name='page_crawled',
                                 obj=response,
                                 return_classes=self.response_model,
                                 links=links or [])

    def request_error(self, request, error):
        self._check_startstop()
        self.logger.manager.debug(self._msg('PAGE_REQUEST_ERROR url=%s' % request.url))
        processed_page = self._process_components(method_name='request_error',
                                                  obj=request,
                                                  return_classes=self.request_model,
                                                  error=error)
        #self.event_log_manager.page_crawled_error(processed_page, error)
        return processed_page

    def get_next_requests(self, max_next_requests=0):
        self._check_startstop()

        # End condition check
        if self.max_requests and self.n_requests >= self.max_requests:
            self.logger.manager.warning(self._msg('MAX PAGES REACHED! (%s/%s)' % (self.n_requests, self.max_requests)))
            self._finished = True
            return []

        # Calculate number of requests
        max_next_requests = max_next_requests or self.max_next_requests
        if self.max_requests:
            if not max_next_requests:
                max_next_requests = self.max_requests - self.n_requests
            else:
                if self.n_requests+max_next_requests > self.max_requests:
                    max_next_requests = self.max_requests - self.n_requests

        # log (in)
        self.logger.manager.debug(self._msg('GET_NEXT_REQUESTS(in) max_next_requests=%s n_requests=%s/%s' %
                                            (max_next_requests, self.n_requests, self.max_requests or '-')))

        # get next requests
        next_requests = self.backend.get_next_requests(max_next_requests)

        # Increment requests counter
        self._n_requests += len(next_requests)

        # Increment Iteration and log event
        if next_requests:
            self._iteration += 1
            self.event_log_manager.get_next_requests(max_next_requests, next_requests)

        # log (out)
        self.logger.manager.debug(self._msg('GET_NEXT_REQUESTS(out) returned_requests=%s n_requests=%s/%s' %
                                            (len(next_requests), self.n_requests, self.max_requests or '-')))

        # Return next requests
        return next_requests

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
        for component_category, component, check_response in self._components_pipeline:
            components = component if isinstance(component, list) else [component]
            for component in components:
                return_obj = self._process_component(component=component, method_name=method_name,
                                                     component_category=component_category, obj=return_obj,
                                                     return_classes=return_classes,
                                                     **kwargs)
                if check_response and obj and not return_obj:
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
