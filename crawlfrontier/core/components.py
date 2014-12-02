from abc import ABCMeta, abstractmethod


class Component(object):
    """Interface definition for a frontier component"""
    __metaclass__ = ABCMeta
    component_name = 'Base Component'

    @abstractmethod
    def frontier_start(self):
        pass

    @abstractmethod
    def frontier_stop(self):
        pass

    @abstractmethod
    def add_seeds(self, seeds):
        pass

    @abstractmethod
    def page_crawled(self, response, links):
        pass

    @abstractmethod
    def request_error(self, page, error):
        pass

    @property
    def name(self):
        return self.component_name


class Backend(Component):
    """Interface definition for a Frontier Backend"""
    __metaclass__ = ABCMeta
    component_name = 'Base Backend'

    @abstractmethod
    def get_next_requests(self, max_n_requests):
        raise NotImplementedError


class Middleware(Component):
    """Interface definition for a Frontier Middleware"""
    __metaclass__ = ABCMeta
    component_name = 'Base Middleware'
