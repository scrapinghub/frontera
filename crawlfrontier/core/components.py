class Component(object):
    component_name = 'Base Component'

    @property
    def name(self):
        return self.component_name

    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass

    def add_seeds(self, seeds):
        raise NotImplementedError

    def page_crawled(self, response, links):
        raise NotImplementedError

    def request_error(self, page, error):
        raise NotImplementedError


class Backend(Component):
    component_name = 'Base Backend'

    def get_next_requests(self, max_n_requests):
        raise NotImplementedError


class Middleware(Component):
    component_name = 'Base Middleware'
