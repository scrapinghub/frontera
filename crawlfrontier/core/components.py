from abc import ABCMeta, abstractmethod


class Component(object):
    """
    Interface definition for a frontier component
    The :class:`Component <crawlfrontier.core.components.Component>` object is the base class for frontier
    :class:`Middleware <crawlfrontier.core.components.Middleware>` and
    :class:`Backend <crawlfrontier.core.components.Backend>` objects.

    :class:`FrontierManager <crawlfrontier.core.manager.FrontierManager>` communicates with the active components
    using the hook methods listed below.

    Implementations are different for  :class:`Middleware <crawlfrontier.core.components.Middleware>` and
    :class:`Backend <crawlfrontier.core.components.Backend>` objects, therefore methods are not fully described here
    but in their corresponding section.

    """
    __metaclass__ = ABCMeta
    component_name = 'Base Component'

    @abstractmethod
    def frontier_start(self, **kwargs):
        """
        Called when the frontier starts, see :ref:`starting/stopping the frontier <frontier-start-stop>`.

        :param ** kwargs: Arbitrary number of extra keyword arguments.
        """
        pass

    @abstractmethod
    def frontier_stop(self, **kwargs):
        """
        Called when the frontier stops, see :ref:`starting/stopping the frontier <frontier-start-stop>`.

        :param ** kwargs: Arbitrary number of extra keyword arguments.
        """
        pass

    @abstractmethod
    def add_seeds(self, seeds):
        """
        This method is called when new seeds are are added to the frontier.

        :param list seeds: A list of :class:`Request <crawlfrontier.core.models.Request>` objects.
        """
        pass

    @abstractmethod
    def page_crawled(self, response, links):
        """
        This method is called each time a page has been crawled.

        :param object response: The :class:`Response <crawlfrontier.core.models.Response>` object for the crawled page.
        :param list links: A list of :class:`Request <crawlfrontier.core.models.Request>` objects generated from \
        the links extracted for the crawled page.
        """
        pass

    @abstractmethod
    def request_error(self, page, error):
        """
        This method is called each time an error occurs when crawling a page

        :param object request: The crawled with error :class:`Request <crawlfrontier.core.models.Request>` object.
        :param string error: A string identifier for the error.
        """
        pass

    @property
    def name(self):
        """
        The component name
        """
        return self.component_name

    @classmethod
    def from_manager(cls, manager):
        """
        Class method called from :class:`FrontierManager <crawlfrontier.core.manager.FrontierManager>` passing the
        manager itself.

        Example of usage::

            def from_manager(cls, manager):
                return cls(settings=manager.settings)

        """
        return cls()


class Backend(Component):
    """Interface definition for a Frontier Backend"""
    __metaclass__ = ABCMeta
    component_name = 'Base Backend'

    @abstractmethod
    def get_next_requests(self, max_n_requests):
        """
        Returns a list of next requests to be crawled.

        :param int max_next_requests: Maximum number of requests to be returned by this method.

        :return: list of :class:`Request <crawlfrontier.core.models.Request>` objects.
        """
        raise NotImplementedError


class Middleware(Component):
    """Interface definition for a Frontier Middlewares"""
    __metaclass__ = ABCMeta
    component_name = 'Base Middleware'
