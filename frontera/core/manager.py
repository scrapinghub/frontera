import logging

from frontera.core import models
from frontera.core.components import (
    Backend,
    CanonicalSolver,
    DistributedBackend,
    Middleware,
)
from frontera.exceptions import NotConfigured
from frontera.settings import Settings
from frontera.utils.misc import load_object


class ComponentsPipelineMixin:
    def __init__(
        self,
        backend,
        middlewares=None,
        canonicalsolver=None,
        db_worker=False,
        strategy_worker=False,
    ):
        self._logger_components = logging.getLogger("manager.components")

        # Load middlewares
        self._middlewares = self._load_middlewares(middlewares)

        # Load canonical solver
        self._logger_components.debug(
            "Loading canonical url solver '%s'", canonicalsolver
        )
        self._canonicalsolver = self._load_object(canonicalsolver)
        assert isinstance(self.canonicalsolver, CanonicalSolver), (
            f"canonical solver '{self.canonicalsolver.__class__.__name__}' must subclass CanonicalSolver"
        )

        # Load backend
        self._logger_components.debug("Loading backend '%s'", backend)
        self._backend = self._load_backend(backend, db_worker, strategy_worker)

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

    def _load_backend(self, backend, db_worker, strategy_worker):
        cls = load_object(backend)
        assert issubclass(cls, Backend), (
            f"backend '{cls.__name__}' must subclass Backend"
        )
        if issubclass(cls, DistributedBackend):
            if db_worker:
                return cls.db_worker(self)
            if strategy_worker:
                return cls.strategy_worker(self)
            raise RuntimeError("Distributed backends are meant to be used in workers.")
        assert not strategy_worker, (
            "In order to distribute backend only DistributedBackend "
            "subclasses are allowed to use."
        )
        if hasattr(cls, "from_manager"):
            return cls.from_manager(self)
        return cls()

    def _load_middlewares(self, middleware_names):
        # TO-DO: Use dict for middleware ordering
        mws = []
        for mw_name in middleware_names or []:
            self._logger_components.debug("Loading middleware '%s'", mw_name)
            try:
                mw = self._load_object(mw_name, silent=False)
                assert isinstance(mw, Middleware), (
                    f"middleware '{mw.__class__.__name__}' must subclass Middleware"
                )
                if mw:
                    mws.append(mw)
            except NotConfigured:
                self._logger_components.warning("middleware '%s' disabled!", mw_name)

        return mws

    def _process_components(self, method_name, obj=None, return_classes=None, **kwargs):
        return_obj = obj
        for component_category, components, check_response in self._components_pipeline:
            component_list = (
                components if isinstance(components, list) else [components]
            )
            for component in component_list:
                result = self._process_component(
                    component=component,
                    method_name=method_name,
                    component_category=component_category,
                    obj=return_obj,
                    return_classes=return_classes,
                    **kwargs,
                )
                if check_response:
                    return_obj = result
                if check_response and obj and not return_obj:
                    self._logger_components.warning(
                        "Object '%s' filtered in '%s' by '%s'",
                        obj.__class__.__name__,
                        method_name,
                        component.__class__.__name__,
                    )
                    return None
        return return_obj

    def _process_component(
        self, component, method_name, component_category, obj, return_classes, **kwargs
    ):
        self._logger_components.debug(
            "processing '%s' '%s.%s' %s",
            method_name,
            component_category,
            component.__class__.__name__,
            obj,
        )
        return_obj = getattr(component, method_name)(*([obj] if obj else []), **kwargs)
        assert return_obj is None or isinstance(return_obj, return_classes), (
            "{} '{}.{}' must return None or {}, Got '{}'".format(
                component_category,
                obj.__class__.__name__,
                method_name,
                " or ".join(c.__name__ for c in return_classes)
                if isinstance(return_classes, tuple)
                else return_classes.__name__,
                return_obj.__class__.__name__,
            )
        )
        return return_obj


class BaseManager:
    def __init__(self, request_model, response_model, settings=None):
        # Settings
        self._settings = settings or Settings()

        # Logger
        self._logger = logging.getLogger("manager")

        # Log frontier manager starting
        self._logger.info("-" * 80)
        self._logger.info("Starting Frontier Manager...")

        # Load request model
        self._request_model = load_object(request_model)
        assert issubclass(self._request_model, models.Request), (
            f"Request model '{self._request_model.__name__}' must subclass 'Request'"
        )

        # Load response model
        self._response_model = load_object(response_model)
        assert issubclass(self._response_model, models.Response), (
            f"Response model '{self._response_model.__name__}' must subclass 'Response'"
        )

    @classmethod
    def from_settings(cls, settings=None):
        manager_settings = Settings(settings)
        return BaseManager(
            request_model=manager_settings.REQUEST_MODEL,
            response_model=manager_settings.RESPONSE_MODEL,
            settings=manager_settings,
        )

    def _load_object(self, obj_class_name, silent=False):
        obj_class = load_object(obj_class_name)
        try:
            return self._load_frontier_object(obj_class)
        except NotConfigured:
            if not silent:
                raise

    def _load_frontier_object(self, obj_class):
        if hasattr(obj_class, "from_manager"):
            return obj_class.from_manager(self)
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


class FrontierManager(BaseManager, ComponentsPipelineMixin):
    """
    The :class:`FrontierManager <frontera.core.manager.FrontierManager>` object encapsulates the whole frontier,
    providing an API to interact with. It's also responsible of loading and communicating all different frontier
    components.
    """

    def __init__(
        self,
        request_model,
        response_model,
        backend,
        middlewares=None,
        test_mode=False,
        max_requests=0,
        max_next_requests=0,
        auto_start=True,
        settings=None,
        canonicalsolver=None,
        db_worker=False,
        strategy_worker=False,
    ):
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

        :param bool db_worker: True if class is instantiated in DB worker environment

        :param bool strategy_worker: True if class is instantiated in strategy worker environment
        """

        BaseManager.__init__(self, request_model, response_model, settings=settings)

        # Test mode
        self._test_mode = test_mode
        self._logger.debug(f"Test mode {'ENABLED' if self.test_mode else 'DISABLED'}")

        # Page counters
        self._max_requests = max_requests
        self._max_next_requests = max_next_requests
        self._n_requests = 0

        # Iteration counter
        self._iteration = 0

        # Manager finished flag
        self._finished = False

        ComponentsPipelineMixin.__init__(
            self,
            backend=backend,
            middlewares=middlewares,
            canonicalsolver=canonicalsolver,
            db_worker=db_worker,
            strategy_worker=strategy_worker,
        )

        # Init frontier components pipeline
        self._components_pipeline = [
            ("Middleware", self.middlewares, True),
            ("CanonicalSolver", self.canonicalsolver, False),
            ("Backend", self.backend, False),
        ]

        # Log frontier manager start
        self._logger.info("Frontier Manager Started!")
        self._logger.info("-" * 80)

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
        return FrontierManager(
            request_model=manager_settings.REQUEST_MODEL,
            response_model=manager_settings.RESPONSE_MODEL,
            backend=manager_settings.BACKEND,
            middlewares=manager_settings.MIDDLEWARES,
            test_mode=manager_settings.TEST_MODE,
            max_requests=manager_settings.MAX_REQUESTS,
            max_next_requests=manager_settings.MAX_NEXT_REQUESTS,
            auto_start=manager_settings.AUTO_START,
            settings=manager_settings,
            canonicalsolver=manager_settings.CANONICAL_SOLVER,
            db_worker=db_worker,
            strategy_worker=strategy_worker,
        )

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
            return self.backend.finished()
        return True

    def start(self):
        """
        Notifies all the components of the frontier start. Typically used for initializations (See \
        :ref:`starting/stopping the frontier <frontier-start-stop>`).

        :return: None.
        """
        assert not self._started, "Frontier already started!"
        self._logger.debug("START")
        self._process_components(method_name="frontier_start")
        self._started = True

    def stop(self):
        """
        Notifies all the components of the frontier stop. Typically used for finalizations (See \
        :ref:`starting/stopping the frontier <frontier-start-stop>`).

        :return: None.
        """
        self._check_startstop()
        self._logger.debug("STOP")
        self._process_components(method_name="frontier_stop")
        self._stopped = True

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
            assert isinstance(seed, self._request_model), (
                f"Seed objects must subclass '{self._request_model.__name__}', '{type(seed).__name__}' found"
            )
        self._logger.debug("ADD_SEEDS urls_length=%d", len(seeds))
        self._process_components(
            method_name="add_seeds", obj=seeds, return_classes=(list,)
        )  # TODO: Dar vuelta

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
            self._logger.info(
                "MAX PAGES REACHED! (%s/%s)", self.n_requests, self.max_requests
            )
            self._finished = True
            return []

        # Calculate number of requests
        max_next_requests = max_next_requests or self.max_next_requests
        if self.max_requests and (
            not max_next_requests
            or self.n_requests + max_next_requests > self.max_requests
        ):
            max_next_requests = self.max_requests - self.n_requests

        # log (in)
        self._logger.debug(
            "GET_NEXT_REQUESTS(in) max_next_requests=%s n_requests=%s/%s",
            max_next_requests,
            self.n_requests,
            self.max_requests or "-",
        )

        # get next requests
        next_requests = self.backend.get_next_requests(max_next_requests, **kwargs)

        # Increment requests counter
        self._n_requests += len(next_requests)

        # Increment iteration
        if next_requests:
            self._iteration += 1

        # log (out)
        self._logger.debug(
            "GET_NEXT_REQUESTS(out) returned_requests=%s n_requests=%s/%s",
            len(next_requests),
            self.n_requests,
            self.max_requests or "-",
        )
        return next_requests

    def page_crawled(self, response):
        """
        Informs the frontier about the crawl result.

        :param object response: The :class:`Response <frontera.core.models.Response>` object for the crawled page.

        :return: None.
        """
        self._check_startstop()
        self._logger.debug(
            "PAGE_CRAWLED url=%s status=%s", response.url, response.status_code
        )
        assert isinstance(response, self.response_model), (
            f"Response object must subclass '{self.response_model.__name__}', '{type(response).__name__}' found"
        )
        assert hasattr(response, "request") and response.request, (
            "Empty response request"
        )
        assert isinstance(response.request, self.request_model), (
            f"Response request object must subclass '{self.request_model.__name__}', "
            f"'{type(response.request).__name__}' found"
        )
        assert isinstance(response, self.response_model), (
            f"Response object must subclass '{self.response_model.__name__}', '{type(response).__name__}' found"
        )
        self._process_components(
            method_name="page_crawled", obj=response, return_classes=self.response_model
        )

    def links_extracted(self, request, links):
        """
        Informs the frontier about extracted links for the request.

        :param object request: The :class:`Request <frontera.core.models.Request>` object from which the links where crawled.
        :param list links: A list of :class:`Request <frontera.core.models.Request>` objects generated from the links \
        extracted for the request.

        :return: None.
        """
        self._check_startstop()
        self._logger.debug("LINKS_EXTRACTED url=%s links=%d", request.url, len(links))
        assert isinstance(request, self.request_model), (
            f"Request object must subclass '{self.request_model.__name__}', '{type(request).__name__}' found"
        )
        for link in links:
            assert isinstance(link, self._request_model), (
                f"Link objects must subclass '{self._request_model.__name__}', '{type(link).__name__}' found"
            )
        self._process_components(
            method_name="links_extracted",
            obj=request,
            return_classes=self.request_model,
            links=links,
        )

    def request_error(self, request, error):
        """
        Informs the frontier about a page crawl error. An error identifier must be provided.

        :param object request: The crawled with error :class:`Request <frontera.core.models.Request>` object.
        :param string error: A string identifier for the error.

        :return: None.
        """
        self._check_startstop()
        self._logger.debug("PAGE_REQUEST_ERROR url=%s error=%s", request.url, error)
        return self._process_components(
            method_name="request_error",
            obj=request,
            return_classes=self.request_model,
            error=error,
        )

    def _check_startstop(self):
        assert self._started, "Frontier not started!"
        assert not self._stopped, "Call to stopped frontier!"
