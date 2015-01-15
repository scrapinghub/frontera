import re
from scrapy import signals
from scrapy.http import Request
from scrapy.utils.httpobj import urlparse_cached
from scrapy import log
from aluanabot.classes.Log.DomainLog import DomainLog
from aluanabot.classes.StopwordsFilter.SoftStopwordsFilter import SoftStopwordsFilter
from aluanabot.settings import *
from crawlfrontier import Middleware


class FilterSoftStopwordsMiddleware(Middleware):

    SOFT_WHITE_STOP_LIST = []
    SOFT_BLACK_STOP_LIST = []

    def __init__(self, manager):
        self.manager = manager
        self.stop_words_filter = SoftStopwordsFilter(soft_white=SOFT_STOPWORDS_FILTER_WHITE, soft_black=SOFT_STOPWORDS_FILTER_BLACK)

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
                should = self.should_follow(link)
                if not should:
                    self.manager.backend.requests[link.meta['fingerprint']] = link
                    log.msg(message='Filtered soft stopword request to '+str(link.url),
                        level=log.DEBUG, domain=str(response.meta['domain']['netloc']), url=str(link.url),
                        module='soft_stopwords', filtered=True)

        return response

    def should_follow(self, request):
        result = self.stop_words_filter.url_matched(request.url)
        if result:
            return False
        else:
            return True

