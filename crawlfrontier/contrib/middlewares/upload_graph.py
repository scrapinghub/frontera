import re

from crawlfrontier.core.components import Middleware
from crawlfrontier import graphs

class UploadGraphMiddleware(Middleware):

    component_name = 'Upload Graph Middleware'

    def __init__(self, manager):
        self.manager = manager

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)


    def frontier_start(self):
        pass


    def frontier_stop(self):
        print '**************************************************************'
        print '**************************************************************'
        print '**************************************************************'
        print '**************************************************************'
        print '**************************************************************'
        print '**************************************************************'
        g = graphs.Manager(engine='sqlite:///my_record.db')
        g.render(filename='graph.png', label='A simple Graph', fontsize=15, node_fontsize=8, node_height=2, node_width=2, node_fixedsize=False)
        pass



    def add_seeds(self, seeds):
        print 1
        return seeds

    def page_crawled(self, response, links):
        print 2
        return response

    def request_error(self, request, error):
        print 3
        return request

