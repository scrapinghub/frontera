import re
from scrapy import signals
from scrapy.http import Request
from scrapy.utils.httpobj import urlparse_cached
from scrapy import log
from aluanabot.settings import *
from aluanabot.classes.Log.DomainLog import DomainLog
from aluanabot.classes.StopwordsFilter.HardStopwordsFilter import HardStopwordsFilter
from crawlfrontier import Middleware
from crawlfrontier.exceptions import NotConfigured
from crawlfrontier.utils.misc import load_object
from crawlfrontier.utils.url import canonicalize_url

class FilterHardStopwordsMiddleware(Middleware):

    HARD_STOP_LIST = []
    fingerprint_function_name = ''

    def __init__(self, manager):
        self.manager = manager
        self.stop_words_filter = HardStopwordsFilter(HARD_STOPWORDS_FILTER)


    @classmethod
    def from_manager(cls, manager):
        return cls(manager)


    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass

    def add_seeds(self, seeds):
        return seeds

    def request_error(self, request, error):
        pass


    def page_crawled(self, response, links):
        for link in links:
            fingerprint = link.meta['fingerprint']
            if fingerprint not in self.manager.backend.requests:
                should, token, term, u = self.should_follow(link)
                if not should:
                    self.manager.backend.requests[link.meta['fingerprint']] = link
                    log.msg(message='Filtered hard stopword request to '+str(link.url),
                        level=log.DEBUG, domain=str(response.meta['domain']['netloc']), url=str(link.url),
                        module='hard_stopwords', token=token, term=term, filter_url=u, filtered=True)
        return response



    def should_follow(self, request):
        result, token, term, u = self.stop_words_filter.url_matched(request)
        if result == True:
            return False, token, term, u
        else:
            return True, token, term, u

