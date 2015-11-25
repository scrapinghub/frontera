from abc import ABCMeta, abstractmethod, abstractproperty


class StartStopMixin(object):
    def frontier_start(self):
        """
        Called when the frontier starts, see :ref:`starting/stopping the frontier <frontier-start-stop>`.
        """
        pass

    def frontier_stop(self):
        """
        Called when the frontier stops, see :ref:`starting/stopping the frontier <frontier-start-stop>`.
        """
        pass


class Metadata(StartStopMixin):
    """Interface definition for frontier metadata"""
    __metaclass__ = ABCMeta

    @abstractmethod
    def add_seeds(self, seeds):
        """
        This method is called when new seeds are are added to the frontier.

        :param list seeds: A list of :class:`Request <frontera.core.models.Request>` objects.
        """
        pass

    @abstractmethod
    def page_crawled(self, response, links):
        """
        This method is called each time a page has been crawled.

        :param object response: The :class:`Response <frontera.core.models.Response>` object for the crawled page.
        :param list links: A list of :class:`Request <frontera.core.models.Request>` objects generated from \
        the links extracted for the crawled page.
        """
        pass

    @abstractmethod
    def request_error(self, page, error):
        """
        This method is called each time an error occurs when crawling a page

        :param object request: The crawled with error :class:`Request <frontera.core.models.Request>` object.
        :param string error: A string identifier for the error.
        """
        pass


class Queue(StartStopMixin):
    """Interface definition for a frontier queue"""
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        """
        Returns a list of next requests to be crawled.

        :param int max_next_requests: Maximum number of requests to be returned by this method.
        :param dict kwargs: A parameters from downloader component.

        :return: list of :class:`Request <frontera.core.models.Request>` objects.
        """
        raise NotImplementedError

    @abstractmethod
    def schedule(self, batch):
        """
        Schedules a new documents for download from batch, and updates score in metadata.

        :param batch: dict, key - hex string fingerprint, value - tuple(score, request, schedule), if ``schedule``
        is True, then document needs to be scheduled for download, False - only update score in metadata.
        """
        raise NotImplementedError

    @abstractmethod
    def count(self):
        """
        Returns count of documents in the queue.
        :return: int
        """
        raise NotImplementedError


class States(StartStopMixin):
    """Interface definition for a frontier document states"""
    __metaclass__ = ABCMeta

    NOT_CRAWLED = 0
    QUEUED = 1
    CRAWLED = 2
    ERROR = 3

    @abstractmethod
    def update_cache(self, objs):
        """
        Reads states from meta['state'] field of request in objs and stores states in internal cache.

        :param objs: list or tuple of :class:`Request <frontera.core.models.Request>` objects.
        """

    @abstractmethod
    def set_states(self, objs):
        """
        Sets meta['state'] field from cache for every request in objs.

        :param objs: list or tuple of :class:`Request <frontera.core.models.Request>` objects.
        """
        raise NotImplementedError

    @abstractmethod
    def flush(self, force_clear):
        """
        Flushes internal cache to storage.

        :param force_clear: boolean, True - signals to clear cache after flush
        """
        raise NotImplementedError

    @abstractmethod
    def fetch(self, fingerprints):
        raise NotImplementedError


class Component(Metadata):
    """
    Interface definition for a frontier component
    The :class:`Component <frontera.core.components.Component>` object is the base class for frontier
    :class:`Middleware <frontera.core.components.Middleware>` and
    :class:`Backend <frontera.core.components.Backend>` objects.

    :class:`FrontierManager <frontera.core.manager.FrontierManager>` communicates with the active components
    using the hook methods listed below.

    Implementations are different for  :class:`Middleware <frontera.core.components.Middleware>` and
    :class:`Backend <frontera.core.components.Backend>` objects, therefore methods are not fully described here
    but in their corresponding section.

    """
    __metaclass__ = ABCMeta
    component_name = 'Base Component'

    @property
    def name(self):
        """
        The component name
        """
        return self.component_name

    @classmethod
    def from_manager(cls, manager):
        """
        Class method called from :class:`FrontierManager <frontera.core.manager.FrontierManager>` passing the
        manager itself.

        Example of usage::

            def from_manager(cls, manager):
                return cls(settings=manager.settings)

        """
        return cls()


class Middleware(Component):
    """Interface definition for a Frontier Middlewares"""
    __metaclass__ = ABCMeta
    component_name = 'Base Middleware'


class CanonicalSolver(Middleware):
    """Interface definition for a Frontera Canonical Solver"""
    __metaclass__ = ABCMeta
    component_name = 'Base CanonicalSolver'


class Backend(Metadata):
    """Interface definition for frontier backend."""
    __metaclass__ = ABCMeta

    @abstractproperty
    def queue(self):
        """
        :return: associated :class:`Queue <frontera.core.components.Queue>` object
        """
        raise NotImplementedError

    @abstractproperty
    def metadata(self):
        """
        :return: associated :class:`Metadata <frontera.core.components.Metadata>` object
        """
        raise NotImplementedError

    @abstractproperty
    def states(self):
        """
        :return: associated :class:`States <frontera.core.components.States>` object
        """
        raise NotImplementedError

    @abstractmethod
    def finished(self):
        """
        Quick check if crawling is finished. Called pretty often, please make sure calls are lightweight.
        :return: boolean
        """
        raise NotImplementedError

    @abstractmethod
    def get_next_requests(self, max_n_requests, **kwargs):
        """
        Returns a list of next requests to be crawled.

        :param int max_next_requests: Maximum number of requests to be returned by this method.
        :param dict kwargs: A parameters from downloader component.

        :return: list of :class:`Request <frontera.core.models.Request>` objects.
        """
        raise NotImplementedError


class Partitioner(object):
    """
    Base class for a partitioner
    """
    def __init__(self, partitions):
        """
        Initialize the partitioner

        Arguments:
            partitions: A list of available partitions (during startup)
        """
        self.partitions = partitions

    def partition(self, key, partitions=None):
        """
        Takes a string key and num_partitions as argument and returns
        a partition to be used for the message

        Arguments:
            key: the key to use for partitioning
            partitions: (optional) a list of partitions.
        """
        raise NotImplementedError('partition function has to be implemented')
