"""
Frontier backend implementation based on the OPIC algorithm adaptated
to HITS scoring.

OPIC stands for On-Line Page Importance Computation and is described
in:

    Adaptive On-Line Page Importance Computation
    Abiteboul S., Preda M., Cobena G.
    2003

HITS stands for Hyperlink-Induced Topic Search, originally described
in:

    Authoritative sources in a hyperlinked environment
    Kleinber J.
    1998

The basic idea of this backend is to crawl pages with a frequency
proportional to a weighted average of the hub and authority scores.
"""
from crawlfrontier import Backend
from crawlfrontier.core.models import Request

from opichits import OpicHits

import graphdb
import scoredb
import pagedb

class OpicHitsBackend(Backend):    
    """Frontier backend implementation based on the OPIC algorithm adaptated
    to HITS scoring
    """

    component_name = 'OPIC-HITS Backend'

    def __init__(self, manager, db_graph=None, db_scores=None, db_pages=None):
        # Adjacency between pages and links
        self._graph = db_graph or graphdb.SQLite()
        # Score database to decide next page to crawl
        self._scores = db_scores or scoredb.SQLite()
        # Additional information (URL, domain) 
        self._pages = db_pages or pagedb.SQLite()

        # Implementation of the algorithm
        self._opic = OpicHits(db_graph=self._graph)

    # FrontierManager interface
    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    # FrontierManager interface
    def frontier_start(self):
        pass

    # FrontierManager interface
    def frontier_stop(self):
        self._graph.close()
        self._scores.close()
        self._pages.close()
        self._opic.close()

    # Add pages
    ####################################################################
    def _add_new_link(self, link):
        """Add a new node to the graph, if not present

        Returns the fingerprint used to add the link
        """         
        fingerprint = link.meta['fingerprint']
        self._graph.add_node(fingerprint)
        self._pages.add(fingerprint,
                        pagedb.PageData(link.url, link.meta['domain_name']))
        self._scores.add(fingerprint, 0.0)

        return fingerprint

    # FrontierManager interface
    def add_seeds(self, seeds):
        """Start crawling from this seeds"""
        for seed in seeds:
            self._add_new_link(seed)

    # FrontierManager interface
    def page_crawled(self, response, links):
        """Add page info to the graph and reset its score"""
        page_fingerprint = response.meta['fingerprint']
        for link in links:
            link_fingerprint = self._add_new_seed(link)        
            self._graph.add_edge(page_fingerprint, link_fingerprint)

        self._scores.set(page_fingerprint, 0.0)

    # FrontierManager interface
    def request_error(self, page, error):
        """TODO"""
        pass

    # Retrieve pages
    ####################################################################
    def _get_request_from_id(self, page_id):
        page_data = self._pages.get(page_id)
        if page_data:
            result = Request(page_data.url)
            result.meta['fingerprint'] = page_id
            result.meta['domain_name'] = page_data.domain
        else:
            result = None

        return result

    # FrontierManager interface
    def get_next_request(self, max_n_requests):
        """Retrieve the next pages to be crawled"""
        self._opic.update()

        for page_id, h_score, a_score in self._opic.iscores():
            score = self._scores.get(page_id)
            self._scores.set(page_id,
                             score + 0.7*h_score + 0.3*a_score)

        return filter(
            lambda x: x!= None,
            [self._get_request_from_id(page_id) for page_id, page_data in 
             self._scores.get_best_scores(max_n_requests)]
        )


    # Just for testing/debugging
    ####################################################################
    def h_scores(self):
        return {self._pages.get(page_id).url: h_score 
                for page_id, h_score, a_score in self._opic.iscores()}

    def a_scores(self):
        return {self._pages.get(page_id).url: a_score 
                for page_id, h_score, a_score in self._opic.iscores()}

    def page_scores(self):
        return {self._pages.get(page_id).url: 0.7*h_score + 0.3*a_score
                for page_id, h_score, a_score in self._opic.iscores()}
