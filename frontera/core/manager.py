from __future__ import absolute_import

import logging
from abc import ABCMeta, abstractmethod
from collections import Iterable

import six

from frontera.core import models
from frontera.core.components import Backend, DistributedBackend, Middleware, CanonicalSolver
from frontera.exceptions import NotConfigured
from frontera.settings import Settings
from frontera.utils.misc import load_object


class BackendMixin(object):
    def __init__(self, backend, db_worker=False, strategy_worker=False):
        # Load backend
        self._logger_components.debug("Loading backend '%s'", backend)
        self._backend = self._load_backend(backend, db_worker, strategy_worker)
        self._backend.frontier_start()

    def _load_backend(self, backend, db_worker, strategy_worker):
        # FIXME remove obsolete
        cls = load_object(backend)
        assert issubclass(cls, Backend), "backend '%s' must subclass Backend" % cls.__name__
        if issubclass(cls, DistributedBackend):
            if db_worker:
                return cls.db_worker(self)
            if strategy_worker:
                return cls.strategy_worker(self)
            return cls.local(self)
        else:
            assert not strategy_worker, "In order to distribute backend only DistributedBackend " \
                                        "subclasses are allowed to use"
        if hasattr(cls, 'from_manager'):
            return cls.from_manager(self)
        else:
            return cls()

    @property
    def backend(self):
        """
        The :class:`Backend <frontera.core.components.Backend>` object to be used by the frontier. \
        Can be defined with :setting:`BACKEND` setting.
        """
        return self._backend

    def close(self):
        self.backend.frontier_stop()


class StrategyMixin(object):
    def __init__(self, strategy_class, strategy_args, scoring_stream):
        self._scoring_stream = scoring_stream if scoring_stream else LocalUpdateScoreStream(self.backend.queue)
        self._states_context = StatesContext(self.backend.states)
        if isinstance(strategy_class, str):
            strategy_class = load_object(strategy_class)
        self._strategy = strategy_class.from_worker(self, strategy_args, self._scoring_stream, self._states_context)

    @property
    def strategy(self):
        return self._strategy

    @property
    def states_context(self):
        return self._states_context

    def close(self):
        self.strategy.close()
        self.states_context.flush()


class ComponentsPipelineMixin(BackendMixin):
    def __init__(self, backend, middlewares=None, canonicalsolver=None, db_worker=False, strategy_worker=False):
        self._logger_components = logging.getLogger("manager.components")

        # Load middlewares
        self._middlewares = self._load_middlewares(middlewares)

        # Load canonical solver
        self._logger_components.debug("Loading canonical url solver '%s'", canonicalsolver)
        if canonicalsolver:
            self._canonicalsolver = self._load_object(canonicalsolver)
            assert isinstance(self.canonicalsolver, CanonicalSolver), \
                "canonical solver '%s' must subclass CanonicalSolver" % self.canonicalsolver.__class__.__name__
        BackendMixin.__init__(self, backend, db_worker, strategy_worker)

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

    def _load_middlewares(self, middleware_names):
        # TO-DO: Use dict for middleware ordering
        mws = []
        for mw_name in middleware_names or []:
            self._logger_components.debug("Loading middleware '%s'", mw_name)
            try:
                mw = self._load_object(mw_name, silent=False)
                assert isinstance(mw, Middleware), "middleware '%s' must subclass Middleware" % mw.__class__.__name__
                if mw:
                    mws.append(mw)
            except NotConfigured:
                self._logger_components.warning("middleware '%s' disabled!", mw_name)

        return mws

    def _process_components(self, method_name, obj=None, return_classes=None, components=None, **kwargs):
        pipeline = self._components_pipeline if components is None else \
            [self._components_pipeline[c] for c in components]
        return_obj = obj
        for component_category, component, check_response in pipeline:
            components = component if isinstance(component, list) else [component]
            for component in components:
                result = self._process_component(component=component, method_name=method_name,
                                                 component_category=component_category, obj=return_obj,
                                                 return_classes=return_classes, **kwargs)
                if check_response:
                    return_obj = result
                if check_response and obj and not return_obj:
                    self._logger_components.warning("Object '%s' filtered in '%s' by '%s'",
                                                    obj.__class__.__name__, method_name, component.__class__.__name__)
                    return
        return return_obj

    def _process_component(self, component, method_name, component_category, obj, return_classes, **kwargs):
        self._logger_components.debug("processing '%s' '%s.%s' %s",
                                      method_name, component_category, component.__class__.__name__, obj)
        return_obj = getattr(component, method_name)(*([obj] if obj else []), **kwargs)
        assert return_obj is None or isinstance(return_obj, return_classes), \
            "%s '%s.%s' must return None or %s, Got '%s'" % \
            (component_category, obj.__class__.__name__, method_name,
             ' or '.join(c.__name__ for c in return_classes)
             if isinstance(return_classes, tuple) else
             return_classes.__name__,
             return_obj.__class__.__name__)
        return return_obj

    def close(self):
        BackendMixin.close(self)
        super(ComponentsPipelineMixin, self).close()


class StrategyComponentsPipelineMixin(ComponentsPipelineMixin, StrategyMixin):
    def __init__(self, backend, strategy_class, strategy_args, scoring_stream, **kwargs):
        super(StrategyComponentsPipelineMixin, self).__init__(backend, **kwargs)
        StrategyMixin.__init__(self, strategy_class, strategy_args, scoring_stream)

    def close(self):
        StrategyMixin.close(self)
        super(StrategyComponentsPipelineMixin, self).close()


class BaseContext(object):
    def __init__(self, request_model, response_model, settings=None):

        # Settings
        self._settings = settings or Settings()

        # Logger
        self._logger = logging.getLogger("manager")

        # Log frontier manager starting
        self._logger.info('-' * 80)
        self._logger.info('Starting Frontier Manager...')

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
        return BaseContext(request_model=manager_settings.REQUEST_MODEL,
                           response_model=manager_settings.RESPONSE_MODEL,
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
    def settings(self):
        """
        The :class:`Settings <frontera.settings.Settings>` object used by the frontier.
        """
        return self._settings


class BaseManager(object):
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

        # log (in)
        self._logger.debug('GET_NEXT_REQUESTS(in) max_next_requests=%s', max_next_requests)

        # get next requests
        next_requests = self.backend.get_next_requests(max_next_requests, **kwargs)

        # log (out)
        self._logger.debug('GET_NEXT_REQUESTS(out) returned_requests=%s', len(next_requests))
        return next_requests

    def page_crawled(self, response):
        """
        Informs the frontier about the crawl result.

        :param object response: The :class:`Response <frontera.core.models.Response>` object for the crawled page.

        :return: None.
        """
        self._logger.debug('PAGE_CRAWLED url=%s status=%s', response.url, response.status_code)
        self._process_components(method_name='page_crawled',
                                 obj=response,
                                 return_classes=self.response_model)

    def links_extracted(self, request, links):
        """
        Informs the frontier about extracted links for the request.

        :param object request: The :class:`Request <frontera.core.models.Request>` object from which the links where crawled.
        :param list links: A list of :class:`Request <frontera.core.models.Request>` objects generated from the links \
        extracted for the request.

        :return: None.
        """
        self._logger.debug('LINKS_EXTRACTED url=%s links=%d', request.url, len(links))
        self._process_components(method_name='links_extracted',
                                 obj=request,
                                 return_classes=self.request_model,
                                 components=(0, 1),
                                 links=links)

    def links_extracted_after(self, request, filtered):
        self._process_components(method_name='links_extracted',
                                 obj=request,
                                 return_classes=self.request_model,
                                 components=(2,),
                                 links=filtered)

    def request_error(self, request, error):
        self._logger.debug('PAGE_REQUEST_ERROR url=%s error=%s', request.url, error)
        return self._process_components(method_name='request_error',
                                        obj=request,
                                        return_classes=self.request_model,
                                        error=error)


class LocalFrontierManager(BaseContext, StrategyComponentsPipelineMixin, BaseManager):
    """
    The :class:`FrontierManager <frontera.core.manager.FrontierManager>` object encapsulates the whole frontier,
    providing an API to interact with. It's also responsible of loading and communicating all different frontier
    components.
    """

    def __init__(self, request_model, response_model, backend, strategy_class, strategy_args, middlewares=None,
                 test_mode=False, max_requests=0, max_next_requests=0, auto_start=True, settings=None,
                 canonicalsolver=None):
        """
        :param object/string request_model: The :class:`Request <frontera.core.models.Request>` object to be \
            used by the frontier.

        :param object/string response_model: The :class:`Response <frontera.core.models.Response>` object to be \
            used by the frontier.

        :param object/string backend: The :class:`Backend <frontera.core.components.Backend>` object to be \
            used by the frontier.

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

        BaseContext.__init__(self, request_model, response_model, settings=settings)

        # Test mode
        self._test_mode = test_mode
        self._logger.debug('Test mode %s' % ("ENABLED" if self.test_mode else "DISABLED"))

        # Page counters
        self._max_requests = max_requests
        self._max_next_requests = max_next_requests
        self._n_requests = 0

        # Iteration counter
        self._iteration = 0

        # Manager finished flag
        self._finished = False

        StrategyComponentsPipelineMixin.__init__(self, backend, strategy_class, strategy_args, None,
                                                 middlewares=middlewares, canonicalsolver=canonicalsolver,
                                                 db_worker=False, strategy_worker=False)

        # Init frontier components pipeline
        # Some code relies on the order, modify carefully
        self._components_pipeline = [
            ('Middleware', self.middlewares, True),
            ('CanonicalSolver', self.canonicalsolver, False),
            ('Strategy', self.strategy, False)
        ]

        # Log frontier manager start
        self._logger.info('Frontier Manager Started!')
        self._logger.info('-' * 80)

        # start/stop
        self._started = False
        self._stopped = False
        self._auto_start = auto_start
        if self.auto_start:
            self.start()

    @classmethod
    def from_settings(cls, settings=None, db_worker=False, strategy_worker=False):
        """
        Returns a :class:`FrontierManager <frontera.core.manager.FrontierManager>`  instance initialized with \
        the passed settings argument. If no settings is given,
        :ref:`frontier default settings <frontier-default-settings>` are used.
        """
        manager_settings = Settings.object_from(settings)
        return LocalFrontierManager(request_model=manager_settings.REQUEST_MODEL,
                                    response_model=manager_settings.RESPONSE_MODEL,
                                    backend=manager_settings.BACKEND,
                                    strategy_class=manager_settings.STRATEGY,
                                    strategy_args=manager_settings.STRATEGY_ARGS,
                                    middlewares=manager_settings.MIDDLEWARES,
                                    test_mode=manager_settings.TEST_MODE,
                                    max_requests=manager_settings.MAX_REQUESTS,
                                    max_next_requests=manager_settings.MAX_NEXT_REQUESTS,
                                    auto_start=manager_settings.AUTO_START,
                                    settings=manager_settings,
                                    canonicalsolver=manager_settings.CANONICAL_SOLVER)

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
        if not self._finished:
            return self.strategy.finished()
        return True

    def start(self):
        """
        Notifies all the components of the frontier start. Typically used for initializations (See \
        :ref:`starting/stopping the frontier <frontier-start-stop>`).

        :return: None.
        """
        assert not self._started, 'Frontier already started!'
        self._logger.debug('START')
        self._process_components(method_name='frontier_start')
        self._started = True

    def stop(self):
        """
        Notifies all the components of the frontier stop. Typically used for finalizations (See \
        :ref:`starting/stopping the frontier <frontier-start-stop>`).

        :return: None.
        """
        self._check_startstop()
        self._logger.debug('STOP')
        self._process_components(method_name='frontier_stop')
        StrategyComponentsPipelineMixin.close(self)
        self._stopped = True

    def add_seeds(self, seeds_file):
        """
        Performs seeds addition procedure. Using file-like object, calls read_seeds method of crawling strategy.

        :param file seeds_file: A file-like object opened in binary mode which will be passed to read_seeds

        :return: None.
        """
        self._check_startstop()
        self.strategy.read_seeds(seeds_file)

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
            self._logger.info('MAX PAGES REACHED! (%s/%s)', self.n_requests, self.max_requests)
            self._finished = True
            return []

        # Calculate number of requests
        max_next_requests = max_next_requests or self.max_next_requests
        if self.max_requests:
            if not max_next_requests:
                max_next_requests = self.max_requests - self.n_requests
            else:
                if self.n_requests + max_next_requests > self.max_requests:
                    max_next_requests = self.max_requests - self.n_requests

        # get next requests
        next_requests = super(LocalFrontierManager, self).get_next_requests(max_next_requests, **kwargs)

        # Increment requests counter
        self._n_requests += len(next_requests)

        # Increment iteration
        if next_requests:
            self._iteration += 1

        return next_requests

    def page_crawled(self, response):
        self._check_startstop()
        assert isinstance(response, self.response_model), "Response object must subclass '%s', '%s' found" % \
                                                          (self.response_model.__name__, type(response).__name__)
        assert hasattr(response, 'request') and response.request, "Empty response request"
        assert isinstance(response.request, self.request_model), "Response request object must subclass '%s', " \
                                                                 "'%s' found" % \
                                                                 (self.request_model.__name__,
                                                                  type(response.request).__name__)
        assert isinstance(response, self.response_model), "Response object must subclass '%s', '%s' found" % \
                                                          (self.response_model.__name__, type(response).__name__)
        self.states_context.to_fetch(response)
        self.states_context.fetch()
        self.states_context.states.set_states(response)
        super(LocalFrontierManager, self).page_crawled(response)
        self.states_context.states.update_cache(response)

    def links_extracted(self, request, links):
        self._check_startstop()
        assert isinstance(request, self.request_model), "Request object must subclass '%s', '%s' found" % \
                                                        (self.request_model.__name__, type(request).__name__)
        for link in links:
            assert isinstance(link, self._request_model), "Link objects must subclass '%s', '%s' found" % \
                                                          (self._request_model.__name__, type(link).__name__)
        super(LocalFrontierManager, self).links_extracted(request, links)
        filtered = self.strategy.filter_extracted_links(request, links)
        if filtered:
            self.states_context.to_fetch(request)
            self.states_context.to_fetch(filtered)
            self.states_context.fetch()
            self.states_context.states.set_states(filtered)
            super(LocalFrontierManager, self).links_extracted_after(request, filtered)
            self.states_context.states.update_cache(filtered)

    def request_error(self, request, error):
        """
        Informs the frontier about a page crawl error. An error identifier must be provided.

        :param object request: The crawled with error :class:`Request <frontera.core.models.Request>` object.
        :param string error: A string identifier for the error.

        :return: None.
        """
        self._check_startstop()
        self.states_context.to_fetch(request)
        self.states_context.fetch()
        self.states_context.states.set_states(request)
        processed_page = super(LocalFrontierManager, self).request_error(request, error)
        self.states_context.states.update_cache(request)
        return processed_page

    def create_request(self, url, method=b'GET', headers=None, cookies=None, meta=None, body=b''):
        """
        Creates request and applies middleware and canonical solver pipelines.

        :param url: str
        :param method: bytes
        :param headers: dict
        :param cookies: dict
        :param meta: dict
        :param body: bytes
        :return: :class:`Request <frontera.core.models.Request>` object
        """
        r = self.request_model(url, method=method, headers=headers, cookies=cookies, meta=meta, body=body)
        self._process_components('create_request',
                                 obj=r,
                                 return_classes=self.request_model,
                                 components=(0, 1))
        return r

    def _check_startstop(self):
        assert self._started, "Frontier not started!"
        assert not self._stopped, "Call to stopped frontier!"


class WorkerFrontierManager(BaseContext, StrategyComponentsPipelineMixin):
    """
    The :class:`WorkerFrontierManager <frontera.core.manager.WorkerFrontierManager>` class role is to
    instantiate the core components and is used mainly by workers.
    """

    def __init__(self, settings, request_model, response_model, backend, max_next_requests, strategy_class=None,
                 strategy_args=None, scoring_stream=None, middlewares=None, canonicalsolver=None, db_worker=False,
                 strategy_worker=False):
        """
        :param object/string request_model: The :class:`Request <frontera.core.models.Request>` object to be \
            used by the frontier.

        :param object/string response_model: The :class:`Response <frontera.core.models.Response>` object to be \
            used by the frontier.

        :param object/string backend: The :class:`Backend <frontera.core.components.Backend>` object to be \
            used by the frontier.

        :param list middlewares: A list of :class:`Middleware <frontera.core.components.Middleware>` \
            objects to be used by the frontier.

        :param int max_next_requests: Maximum number of requests returned by \
            :attr:`get_next_requests <frontera.core.manager.FrontierManager.get_next_requests>` method.

        :param object/string settings: The :class:`Settings <frontera.settings.Settings>` object used by \
            the frontier.

        :param object/string canonicalsolver: The :class:`CanonicalSolver <frontera.core.components.CanonicalSolver>`
            object to be used by frontier.
        :param object scoring_stream: Instance of :class:`UpdateScoreStream <frontera.core.manager.UpdateScoreStream>`
            for crawling strategy to send scheduled requests to.

        :param bool db_worker: True if class is instantiated in DB worker environment

        :param bool strategy_worker: True if class is instantiated in strategy worker environment
        """

        BaseContext.__init__(self, request_model, response_model, settings=settings)

        self._max_next_requests = max_next_requests
        if strategy_worker:
            StrategyComponentsPipelineMixin.__init__(self, backend, strategy_class, strategy_args, scoring_stream,
                                                     middlewares=middlewares, canonicalsolver=canonicalsolver,
                                                     db_worker=db_worker, strategy_worker=strategy_worker)
            # Init frontier components pipeline
            # Some code relies on the order, modify carefully
            self._components_pipeline = [
                ('Middleware', self.middlewares, True),
                ('CanonicalSolver', self.canonicalsolver, False),
            ]
        if db_worker:
            ComponentsPipelineMixin.__init__(self, backend, db_worker=db_worker, strategy_worker=strategy_worker)

        # Log frontier manager start
        self._logger.info('Frontier Manager Started!')
        self._logger.info('-' * 80)

    @classmethod
    def from_settings(cls, settings=None, db_worker=False, strategy_worker=False, scoring_stream=None):
        manager_settings = Settings.object_from(settings)
        kwargs = {
            'request_model': manager_settings.REQUEST_MODEL,
            'response_model': manager_settings.RESPONSE_MODEL,
            'backend': manager_settings.BACKEND,
            'max_next_requests': manager_settings.MAX_NEXT_REQUESTS,
            'settings': manager_settings,
            'db_worker': db_worker,
            'strategy_worker': strategy_worker
        }
        if strategy_worker:
            kwargs.update({
                'strategy_class': manager_settings.STRATEGY,
                'strategy_args': manager_settings.STRATEGY_ARGS,
                'middlewares': manager_settings.MIDDLEWARES,
                'canonicalsolver': manager_settings.CANONICAL_SOLVER,
                'scoring_stream': scoring_stream
            })
        return WorkerFrontierManager(**kwargs)

    @property
    def test_mode(self):
        return False

    def create_request(self, url, method=b'GET', headers=None, cookies=None, meta=None, body=b''):
        """
        Creates request and applies middleware and canonical solver pipelines.

        :param url: str
        :param method: bytes
        :param headers: dict
        :param cookies: dict
        :param meta: dict
        :param body: bytes
        :return: :class:`Request <frontera.core.models.Request>` object
        """
        r = self.request_model(url, method=method, headers=headers, cookies=cookies, meta=meta, body=body)
        return self._process_components('create_request',
                                        obj=r,
                                        return_classes=self.request_model,
                                        components=(0, 1))


class SpiderFrontierManager(BaseContext, ComponentsPipelineMixin, BaseManager):

    def __init__(self, request_model, response_model, backend, middlewares, max_next_requests, settings,
                 canonicalsolver):
        BaseContext.__init__(self, request_model, response_model, settings=settings)
        ComponentsPipelineMixin.__init__(self, backend, middlewares=middlewares, canonicalsolver=canonicalsolver,
                                         db_worker=False, strategy_worker=False)

        self.max_next_requests = max_next_requests
        self._components_pipeline = [
            ('Middleware', self.middlewares, True),
            ('CanonicalSolver', self.canonicalsolver, False),
            ('Backend', self.backend, False)
        ]

    @classmethod
    def from_settings(cls, settings=None):
        manager_settings = Settings.object_from(settings)
        return SpiderFrontierManager(request_model=manager_settings.REQUEST_MODEL,
                                     response_model=manager_settings.RESPONSE_MODEL,
                                     backend=manager_settings.BACKEND,
                                     middlewares=manager_settings.MIDDLEWARES,
                                     max_next_requests=manager_settings.MAX_NEXT_REQUESTS,
                                     settings=manager_settings,
                                     canonicalsolver=manager_settings.CANONICAL_SOLVER)

    @property
    def test_mode(self):
        return False

    @property
    def auto_start(self):
        return True

    def get_next_requests(self, max_next_requests=0, **kwargs):
        return super(SpiderFrontierManager, self).get_next_requests(max_next_requests=max_next_requests or self.max_next_requests, **kwargs)

    def links_extracted(self, request, links):
        super(SpiderFrontierManager, self).links_extracted(request, links)
        super(SpiderFrontierManager, self).links_extracted_after(request, links)

    @property
    def finished(self):
        return False

    def start(self):
        self._logger.debug('START')
        self._process_components(method_name='frontier_start')

    def stop(self):
        super(SpiderFrontierManager, self).close()


@six.add_metaclass(ABCMeta)
class UpdateScoreStream(object):
    @abstractmethod
    def send(self, request, score=1.0, dont_queue=False):
        pass

    def flush(self):
        pass


class MessageBusUpdateScoreStream(UpdateScoreStream):
    def __init__(self, producer, encoder):
        self._producer = producer
        self._encoder = encoder

    def send(self, request, score=1.0, dont_queue=False):
        encoded = self._encoder.encode_update_score(
            request=request,
            score=score,
            schedule=not dont_queue
        )
        self._producer.send(None, encoded)


class LocalUpdateScoreStream(UpdateScoreStream):
    def __init__(self, queue):
        self._queue = queue

    def send(self, request, score=1.0, dont_queue=False):
        self._queue.schedule([(request.meta[b'fingerprint'], score, request, not dont_queue)])


class StatesContext(object):
    def __init__(self, states):
        self._requests = []
        self.states = states
        self._fingerprints = dict()
        self.logger = logging.getLogger("states-context")

    def to_fetch(self, requests):
        requests = requests if isinstance(requests, Iterable) else [requests]
        for request in requests:
            fingerprint = request.meta[b'fingerprint']
            self._fingerprints[fingerprint] = request

    def fetch(self):
        self.states.fetch(self._fingerprints)
        self._fingerprints.clear()

    def refresh_and_keep(self, requests):
        self.to_fetch(requests)
        self.fetch()
        self.states.set_states(requests)
        self._requests.extend(requests if isinstance(requests, Iterable) else [requests])

    def release(self):
        self.states.update_cache(self._requests)
        self._requests = []

    def flush(self):
        self.logger.info("Flushing states")
        self.states.flush()
        self.logger.info("Flushing of states finished")
