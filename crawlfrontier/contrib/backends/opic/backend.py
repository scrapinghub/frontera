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
from crawlfrontier.core.models import Page

from opichits import OpicHits

import graphdb
import scoredb
import pagedb

class OpicHitsBackend(Backend):    
    """Frontier backend implementation based on the OPIC algorithm adaptated
    to HITS scoring
    """

    def __init__(self, manager):
        # Adjacency between pages and links
        self._graph = graphdb.SQLite()
        # Score database to decide next page to crawl
        self._scores = scoredb.SQLite()
        # Additional information (URL, domain) 
        self._pages = pagedb.SQLite()

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
        """Add a new node to the graph, if not present"""         
        self._graph.add_node(link.fingerprint)
        self._pages.add(link.fingerprint,
                        pagedb.PageData(link.url, link.domain.name))
        self._scores.add(link.fingerprint, 0.0)

    # FrontierManager interface
    def add_seeds(self, links):
        """Start crawling from this links"""
        for link in links:
            self._add_new_link(link)

    # FrontierManager interface
    def page_crawled(self, page, links):
        """Add page info to the graph and reset its score"""
        for link in links:
            self._add_new_link(link)
            self._graph.add_edge(page.fingerprint, link.fingerprint)

        self._scores.set(page.fingerprint, 0.0)
        return page

    # FrontierManager interface
    def page_crawled_error(self, page, error):
        """TODO"""
        return page

    # Retrieve pages
    ####################################################################
    def _get_page_from_id(self, page_id):
        page_data = self._pages.get(page_id)
        if page_data:
            result = Page(page_data.url)
            result.fingerprint = page_id
            result.domain = page_data.domain
        else:
            result = None

        return result

    # FrontierManager interface
    def get_page(self, link):
        """Build a Page object from stored information"""
        return self._get_page_from_id(link.fingerprint)

    # FrontierManager interface
    def get_next_pages(self, max_next_pages):
        """Retrieve the next pages to be crawled"""
        self._opic.update()
        for page_id, h_score, a_score in self._opic.iscores():
            score = self._scores.get(page_id)
            self._scores.set(page_id,
                             score + 0.7*h_score + 0.3*a_score)

        return filter(
            lambda x: x!= None,
            [self._get_page_from_id(page_id) for page_id, page_data in 
             self._scores.get_best_scores(max_next_pages)]
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
