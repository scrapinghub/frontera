class Component(object):
    component_name = 'Base Component'

    @property
    def name(self):
        return self.__component_name__

    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass

    def add_seeds(self, links):
        raise NotImplementedError

    def page_crawled(self, page, links):
        raise NotImplementedError

    def page_crawled_error(self, page, error):
        raise NotImplementedError

    def get_page(self, link):
        raise NotImplementedError


class Backend(Component):
    component_name = 'Base Backend'


class Middleware(Component):
    component_name = 'Base Middleware'
