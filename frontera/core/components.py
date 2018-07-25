from __future__ import absolute_import

import six
from abc import ABCMeta, abstractmethod


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


@six.add_metaclass(ABCMeta)
class Metadata(StartStopMixin):
    """Interface definition for a frontier metadata class. This class is responsible for storing documents metadata,
    including content and optimized for write-only data flow."""

    @abstractmethod
    def page_crawled(self, response):
        """
        This method is called every time a page has been crawled.

        :param object response: The :class:`Response <frontera.core.models.Response>` object for the crawled page.
        """
        pass

    @abstractmethod
    def links_extracted(self, request, links):
        """
        This method is called every time a links extracted from a document.

        :param object request: The original :class:`Request <frontera.core.models.Request>` object for the crawled page.
        :param list links: A list of :class:`Request <frontera.core.models.Request>` objects containing extracted links.
        """
        pass

    @abstractmethod
    def request_error(self, page, error):
        """
        This method is called each time an error occurs when crawling a page.

        :param object request: The crawled with error :class:`Request <frontera.core.models.Request>` object.
        :param string error: A string identifier for the error.
        """
        pass


@six.add_metaclass(ABCMeta)
class Queue(StartStopMixin):
    """Interface definition for a frontier queue class. The queue has priorities and partitions."""

    @abstractmethod
    def get_next_requests(self, max_n_requests, partition_id, **kwargs):
        """
        Returns a list of next requests to be crawled, and excludes them from internal storage.

        :param int max_next_requests: Maximum number of requests to be returned by this method.
        :param dict kwargs: A parameters from downloader component.

        :return: list of :class:`Request <frontera.core.models.Request>` objects.
        """
        raise NotImplementedError

    @abstractmethod
    def schedule(self, batch):
        """
        Schedules a new documents for download from batch, and updates score in metadata.

        :param batch: list of tuples(fingerprint, score, request, schedule), if ``schedule`` is True, then document
            needs to be scheduled for download, False - only update score in metadata.
        """
        raise NotImplementedError

    @abstractmethod
    def count(self):
        """
        Returns count of documents in the queue.

        :return: int
        """
        raise NotImplementedError


@six.add_metaclass(ABCMeta)
class States(StartStopMixin):
    """Interface definition for a link states management class. This class is responsible for providing actual
    link state, and persist the state changes in batch-oriented manner."""

    NOT_CRAWLED = 0
    QUEUED = 1
    CRAWLED = 2
    ERROR = 3
    DEFAULT = NOT_CRAWLED

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
    def flush(self):
        """
        Flushes internal cache to storage.
        """
        raise NotImplementedError

    @abstractmethod
    def fetch(self, fingerprints):
        """
        Get states from the persistent storage to internal cache.

        :param fingerprints: list document fingerprints, which state to read
        """
        raise NotImplementedError


@six.add_metaclass(ABCMeta)
class DomainMetadata(StartStopMixin):
    """
    Interface definition for a domain metadata storage. It's main purpose is to store the per-domain metadata using
    Python-friendly structures. Meant to be used by crawling strategy to store counters and flags in low level
    facilities provided by Backend.
    """

    @abstractmethod
    def __setitem__(self, key, value):
        """
        Puts key, value tuple in storage.

        :param key: str
        :param value: Any
        """
        raise NotImplementedError

    @abstractmethod
    def __getitem__(self, key):
        """
        Retrieves the value associated with the storage. Raises KeyError if key is absent.

        :param key: str
        :return value: Any
        """
        raise NotImplementedError

    @abstractmethod
    def __delitem__(self, key):
        """
        Removes the tuple associated with key from storage. Raises KeyError if key is absent.

        :param key: str
        """
        raise NotImplementedError

    def __contains__(self, key):
        """
        Checks if key is present in the storage.

        :param key: str
        :return: boolean
        """
        raise NotImplementedError


@six.add_metaclass(ABCMeta)
class Component(Metadata):
    """
    Interface definition for a frontier component
    The :class:`Component <frontera.core.components.Component>` object is the base class for frontier
    :class:`Middleware <frontera.core.components.Middleware>` and
    :class:`Backend <frontera.core.components.Backend>` objects.

    :class:`FrontierManager <frontera.core.manager.FrontierManager>` communicates with the active components
    using the hook methods listed below.

    Implementations are different for :class:`Middleware <frontera.core.components.Middleware>` and
    :class:`Backend <frontera.core.components.Backend>` objects, therefore methods are not fully described here
    but in their corresponding section.

    """
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


@six.add_metaclass(ABCMeta)
class Middleware(Component):
    """Interface definition for a Frontier Middlewares"""
    component_name = 'Base Middleware'

    def create_request(self, request):
        """
        Applying middleware logic on newly created request.

        :param reqeust: :class:`Request <frontera.core.models.Request>` object
        :return: an instance of :class:`Request <frontera.core.models.Request>` object.
        """
        pass


@six.add_metaclass(ABCMeta)
class CanonicalSolver(Middleware):
    """Interface definition for a Frontera Canonical Solver"""
    component_name = 'Base CanonicalSolver'


class PropertiesMixin(object):
    @property
    @abstractmethod
    def queue(self):
        """
        :return: associated :class:`Queue <frontera.core.components.Queue>` object
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def metadata(self):
        """
        :return: associated :class:`Metadata <frontera.core.components.Metadata>` object
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def states(self):
        """
        :return: associated :class:`States <frontera.core.components.States>` object
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def domain_metadata(self):
        """
        :return: associated :class:`DomainMetadata <frontera.core.components.DomainMetadata>` object
        """
        raise NotImplementedError


@six.add_metaclass(ABCMeta)
class Backend(PropertiesMixin, Component):
    """Interface definition for frontier backend."""

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


@six.add_metaclass(ABCMeta)
class DistributedBackend(Backend):
    """Interface definition for distributed frontier backend. Implies using in strategy worker and DB worker."""

    @classmethod
    def strategy_worker(cls, manager):
        raise NotImplementedError

    @classmethod
    def db_worker(cls, manager):
        raise NotImplementedError

    @classmethod
    def local(cls, manager):
        raise NotImplementedError

    def get_stats(self):
        """
        Returns a dictionary with distributed backend stats.

        Depending on a backend type the method may return different stats to be sent to a message bus.
        Called by :class:`StatsExportMixin <frontera.contrib.messagebus.stats.StatsExportMixin>` for workers.

        :return: dict of stats key/values.
        """
        return None


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


