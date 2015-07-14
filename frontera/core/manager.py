from collections import OrderedDict

from frontera.exceptions import NotConfigured
from frontera.utils.misc import load_object
from frontera.settings import Settings, BaseSettings
from frontera.core.components import Backend, Middleware, CanonicalSolver
from frontera.logger import FrontierLogger
from frontera.core import models


class ComponentsPipelineMixin(object):
    def __init__(self, backend, middlewares=None, canonicalsolver=None):
        # Load middlewares
        self._middlewares = self._load_middlewares(middlewares)

        # Load canonical solver
        self.logger.manager.debug("Loading canonical url solver '%s'" % canonicalsolver)
        self._canonicalsolver = self._load_object(canonicalsolver)
        assert isinstance(self.canonicalsolver, CanonicalSolver), \
            "canonical solver '%s' must subclass CanonicalSolver" % self.canonicalsolver.__class__.__name__

        # Load backend
        self.logger.manager.debug("Loading backend '%s'" % backend)
        self._backend = self._load_object(backend)
        assert isinstance(self.backend, Backend), "backend '%s' must subclass Backend" % \
                                                  self.backend.__class__.__name__

    @property
    def canonicalsolver(self):
        """
        Instance of CanonicalSolver used for getting canonical urls in frontier components.
        """
        return self._canonicalsolver

    @property
    def middlewares(self):
        """
        A list of :class:`Middleware <frontera.core.components.Middleware>` objects to be used by the frontier. \
        Can be defined with :setting:`MIDDLEWARES` setting.
        """
        return self._middlewares

    @property
    def backend(self):
        """
        The :class:`Backend <frontera.core.components.Backend>` object to be used by the frontier. \
        Can be defined with :setting:`BACKEND` setting.
        """
        return self._backend

    def _load_middlewares(self, middleware_names):
        # TO-DO: Use dict for middleware ordering
        mws = []
        for mw_name in middleware_names or []:
            self.logger.manager.debug("Loading middleware '%s'" % mw_name)
            try:
                mw = self._load_object(mw_name, silent=False)
                assert isinstance(mw, Middleware), "middleware '%s' must subclass Middleware" % mw.__class__.__name__
                if mw:
                    mws.append(mw)
            except NotConfigured:
                self.logger.manager.warning("middleware '%s' disabled!" % mw_name)

        return mws

    def _process_components(self, method_name, obj=None, return_classes=None, **kwargs):
        return_obj = obj
        for component_category, component, check_response in self._components_pipeline:
            components = component if isinstance(component, list) else [component]
            for component in components:
                result = self._process_component(component=component, method_name=method_name,
                                                 component_category=component_category, obj=return_obj,
                                                 return_classes=return_classes, **kwargs)
                if check_response:
                    return_obj = result
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


class BaseManager(object):
    def __init__(self, request_model, response_model, logger, settings=None):

        # Settings
        self._settings = settings or Settings()

        # Logger
        self._logger = load_object(logger)(self._settings)
        assert isinstance(self._logger, FrontierLogger), "logger '%s' must subclass FrontierLogger" % \
                                                         self._logger.__class__.__name__

        # Log frontier manager starting
        self.logger.manager.debug('-'*80)
        self.logger.manager.debug('Starting Frontier Manager...')

        # Load request model
        self._request_model = load_object(request_model)
        assert issubclass(self._request_model, models.Request), "Request model '%s' must subclass 'Request'" % \
                                                                self._request_model.__name__

        # Load response model
        self._response_model = load_object(response_model)
        assert issubclass(self._response_model, models.Response), "Response model '%s' must subclass 'Response'" % \
                                                                  self._response_model.__name__

    @classmethod
    def from_settings(cls, settings=None):
        manager_settings = Settings(settings)
        return BaseManager(request_model=manager_settings.REQUEST_MODEL,
                           response_model=manager_settings.RESPONSE_MODEL,
                           logger=manager_settings.LOGGER,
                           settings=manager_settings)

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

    @property
    def request_model(self):
        """
        The :class:`Request <frontera.core.models.Request>` object to be used by the frontier. \
        Can be defined with :setting:`REQUEST_MODEL` setting.
        """
        return self._request_model

    @property
    def response_model(self):
        """
        The :class:`Response <frontera.core.models.Response>` object to be used by the frontier. \
        Can be defined with :setting:`RESPONSE_MODEL` setting.
        """
        return self._response_model

    @property
    def logger(self):
        """
        The :class:`Logger` object to be used by the frontier. Can be defined with :setting:`LOGGER` setting.
        """
        return self._logger

    @property
    def settings(self):
        """
        The :class:`Settings <frontera.settings.Settings>` object used by the frontier.
        """
        return self._settings


class FrontierManager(BaseManager, ComponentsPipelineMixin):
    """
    The :class:`FrontierManager <frontera.core.manager.FrontierManager>` object encapsulates the whole frontier,
    providing an API to interact with. It's also responsible of loading and communicating all different frontier
    components.
    """
    def __init__(self, request_model, response_model, backend, logger, event_log_manager, middlewares=None,
                 test_mode=False, max_requests=0, max_next_requests=0, auto_start=True, settings=None,
                 canonicalsolver=None):
        """
        :param object/string request_model: The :class:`Request <frontera.core.models.Request>` object to be \
        used by the frontier.

        :param object/string response_model: The :class:`Response <frontera.core.models.Response>` object to be \
        used by the frontier.

        :param object/string backend: The :class:`Backend <frontera.core.components.Backend>` object to be \
        used by the frontier.

        :param object/string logger: The :class:`Logger` object to be used by the frontier.

        :param object/string event_log_manager: The :class:`EventLogger` object to be used by the frontier.

        :param list middlewares: A list of :class:`Middleware <frontera.core.components.Middleware>` \
        objects to be used by the frontier.

        :param bool test_mode: Activate/deactivate :ref:`frontier test mode <frontier-test-mode>`.

        :param int max_requests: Number of pages after which the frontier would stop (See \
        :ref:`Finish conditions <frontier-finish>`).

        :param int max_next_requests: Maximum number of requests returned by \
        :attr:`get_next_requests <frontera.core.manager.FrontierManager.get_next_requests>` method.

        :param bool auto_start: Activate/deactivate automatic frontier start (See :ref:`starting/stopping the \
        frontier <frontier-start-stop>`).

        :param object/string settings: The :class:`Settings <frontera.settings.Settings>` object used by \
        the frontier.

        :param object/string canonicalsolver: The :class:`CanonicalSolver <frontera.core.components.CanonicalSolver>`
        object to be used by frontier.
        """

        BaseManager.__init__(self, request_model, response_model, logger, settings=settings)
        ComponentsPipelineMixin.__init__(self, backend=backend, middlewares=middlewares, canonicalsolver=canonicalsolver)

        # Init frontier components pipeline
        self._components_pipeline = [
            ('Middleware', self.middlewares, True),
            ('Backend', self.backend, False),
            ('CanonicalSolver', self.canonicalsolver, False)
        ]

        # Log frontier manager starting
        self.logger.manager.debug('-'*80)
        self.logger.manager.debug('Starting Frontier Manager...')

        # Test mode
        self._test_mode = test_mode
        self.logger.manager.debug('Test mode %s' % ("ENABLED" if self.test_mode else "DISABLED"))

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
        """
        Returns a :class:`FrontierManager <frontera.core.manager.FrontierManager>`  instance initialized with \
        the passed settings argument. If no settings is given,
        :ref:`frontier default settings <frontier-default-settings>` are used.
        """
        manager_settings = Settings.object_from(settings)
        return FrontierManager(request_model=manager_settings.REQUEST_MODEL,
                               response_model=manager_settings.RESPONSE_MODEL,
                               backend=manager_settings.BACKEND,
                               logger=manager_settings.LOGGER,
                               event_log_manager=manager_settings.EVENT_LOG_MANAGER,
                               middlewares=manager_settings.MIDDLEWARES,
                               test_mode=manager_settings.TEST_MODE,
                               max_requests=manager_settings.MAX_REQUESTS,
                               max_next_requests=manager_settings.MAX_NEXT_REQUESTS,
                               auto_start=manager_settings.AUTO_START,
                               settings=manager_settings,
                               canonicalsolver=manager_settings.CANONICAL_SOLVER)

    @property
    def event_log_manager(self):
        """
        The :class:`EventLogger` object to be used by the frontier. \
        Can be defined with :setting:`EVENT_LOGGER` setting.
        """
        return self._event_log_manager

    @property
    def test_mode(self):
        """
        Boolean value indicating if the frontier is using :ref:`frontier test mode <frontier-test-mode>`. \
        Can be defined with :setting:`TEST_MODE` setting.
        """
        return self._test_mode

    @property
    def max_requests(self):
        """
        Number of pages after which the frontier would stop (See :ref:`Finish conditions <frontier-finish>`). \
        Can be defined with :setting:`MAX_REQUESTS` setting.
        """
        return self._max_requests

    @property
    def max_next_requests(self):
        """
        Maximum number of requests returned by \
        :attr:`get_next_requests <frontera.core.manager.FrontierManager.get_next_requests>` method. \
        Can be defined with :setting:`MAX_NEXT_REQUESTS` setting.
        """
        return self._max_next_requests

    @property
    def auto_start(self):
        """
        Boolean value indicating if automatic frontier start is activated. \
        See :ref:`starting/stopping the frontier <frontier-start-stop>`. \
        Can be defined with :setting:`AUTO_START` setting.
        """
        return self._auto_start

    @property
    def iteration(self):
        """
        Current :ref:`frontier iteration <frontier-iterations>`.
        """
        return self._iteration

    @property
    def n_requests(self):
        """
        Number of accumulated requests returned by the frontier.
        """
        return self._n_requests

    @property
    def finished(self):
        """
        Boolean value indicating if the frontier has finished. See :ref:`Finish conditions <frontier-finish>`.
        """
        return self._finished

    def start(self):
        """
        Notifies all the components of the frontier start. Typically used for initializations (See \
        :ref:`starting/stopping the frontier <frontier-start-stop>`).

        :return: None.
        """
        assert not self._started, 'Frontier already started!'
        #self.event_log_manager.frontier_start()
        self.logger.manager.debug(self._msg('START'))
        self._process_components(method_name='frontier_start')
        self._started = True

    def stop(self):
        """
        Notifies all the components of the frontier stop. Typically used for finalizations (See \
        :ref:`starting/stopping the frontier <frontier-start-stop>`).

        :return: None.
        """
        self._check_startstop()
        self.logger.manager.debug(self._msg('STOP'))
        self._process_components(method_name='frontier_stop')
        self._stopped = True
        #self.event_log_manager.frontier_stop()

    def add_seeds(self, seeds):
        """
        Adds a list of seed requests (seed URLs) as entry point for the crawl.

        :param list seeds: A list of :class:`Request <frontera.core.models.Request>` objects.

        :return: None.
        """
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

    def get_next_requests(self, max_next_requests=0, **kwargs):
        """
        Returns a list of next requests to be crawled. Optionally a maximum number of pages can be passed. If no
        value is passed, \
        :attr:`FrontierManager.max_next_requests <frontera.core.manager.FrontierManager.max_next_requests>`
        will be used instead. (:setting:`MAX_NEXT_REQUESTS` setting).

        :param int max_next_requests: Maximum number of requests to be returned by this method.
        :param dict kwargs: Arbitrary arguments that will be passed to backend.

        :return: list of :class:`Request <frontera.core.models.Request>` objects.
        """
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
        next_requests = self.backend.get_next_requests(max_next_requests, **kwargs)

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

    def page_crawled(self, response, links=None):
        """
        Informs the frontier about the crawl result and extracted links for the current page.

        :param object response: The :class:`Response <frontera.core.models.Response>` object for the crawled page.
        :param list links: A list of :class:`Request <frontera.core.models.Request>` objects generated from \
        the links extracted for the crawled page.

        :return: None.
        """
        self._check_startstop()
        self.logger.manager.debug(self._msg('PAGE_CRAWLED url=%s status=%s links=%s' %
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
        """
        Informs the frontier about a page crawl error. An error identifier must be provided.

        :param object request: The crawled with error :class:`Request <frontera.core.models.Request>` object.
        :param string error: A string identifier for the error.

        :return: None.
        """
        self._check_startstop()
        self.logger.manager.error(self._msg('PAGE_REQUEST_ERROR url=%s error=%s' % (request.url, error)))
        processed_page = self._process_components(method_name='request_error',
                                                  obj=request,
                                                  return_classes=self.request_model,
                                                  error=error)
        #self.event_log_manager.page_crawled_error(processed_page, error)
        return processed_page

    def _msg(self, msg):
        return '(%s) %s' % (self.iteration, msg)

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
