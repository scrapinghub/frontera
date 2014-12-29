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
import datetime
import os
import time

from crawlfrontier import Backend
from crawlfrontier.core.models import Request

from opichits import OpicHits

import graphdb
import freqdb
import pagedb
import hitsdb
import hashdb
import updatesdb
import freqest
import pagechange


class OpicHitsBackend(Backend):
    """Frontier backend implementation based on the OPIC algorithm adaptated
    to HITS scoring
    """

    DEFAULT_FREQ = 1.0/(30.0*24*3600.0)  # once a month

    component_name = 'OPIC-HITS Backend'

    def __init__(
            self,
            manager,
            db_graph=None,
            db_freqs=None,
            db_pages=None,
            db_hits=None,
            freq_estimator=None,
            change_detector=None,
            test=False
    ):

        # Adjacency between pages and links
        self._graph = db_graph or graphdb.SQLite()
        # Frequency database to decide next page to crawl
        self._freqs = db_freqs or freqdb.SQLite()
        # Additional information (URL, domain)
        self._pages = db_pages or pagedb.SQLite()
        # Implementation of the OPIC algorithm
        self._opic = OpicHits(
            db_graph=self._graph,
            db_scores=db_hits or hitsdb.SQLite(),
            time_window=1000.0
        )

        self._freqest = freq_estimator
        self._pagechange = change_detector

        self._test = test
        self._manager = manager

    # FrontierManager interface
    @classmethod
    def from_manager(cls, manager):

        in_memory = manager.settings.get('BACKEND_OPIC_IN_MEMORY', False)
        if not in_memory:
            now = datetime.datetime.utcnow()
            workdir = manager.settings.get(
                'BACKEND_OPIC_WORKDIR',
                'crawl-opic-D{0}.{1:02d}.{2:02d}-T{3:02d}.{4:02d}.{5:02d}'.format(
                    now.year,
                    now.month,
                    now.day,
                    now.hour,
                    now.minute,
                    now.second
                )
            )
            if not os.path.isdir(workdir):
                os.mkdir(workdir)

            db_graph = graphdb.SQLite(
                os.path.join(workdir, 'graph.sqlite')
            )
            db_pages = pagedb.SQLite(
                os.path.join(workdir, 'pages.sqlite')
            )
            db_freqs = freqdb.SQLite(
                os.path.join(workdir, 'freqs.sqlite')
            )
            db_hits = hitsdb.SQLite(
                os.path.join(workdir, 'hits.sqlite')
            )

            db_updates = updatesdb.SQLite(
                os.path.join(workdir, 'updates.sqlite')
            )
            db_hash = hashdb.SQLite(
                os.path.join(workdir, 'hash.sqlite')
            )

            manager.logger.backend.debug(
                'OPIC backend workdir: {0}'.format(workdir))
        else:
            db_graph = None
            db_pages = None
            db_freqs = None
            db_hits = None
            db_updates = None
            db_hash = None
            manager.logger.backend.debug('OPIC backend workdir: in-memory')

        return cls(manager, db_graph, db_freqs, db_pages, db_hits,
                   freqest.Simple(db=db_updates),
                   pagechange.BodySHA1(db=db_hash),
                   test=manager.settings.get('BACKEND_TEST', False))

    # FrontierManager interface
    def frontier_start(self):
        pass

    # FrontierManager interface
    def frontier_stop(self):
        if self._test:
            self._h_scores = self.h_scores()
            self._a_scores = self.a_scores()

        self._graph.close()
        self._freqs.close()
        self._pages.close()
        self._opic.close()

    # Add pages
    ####################################################################
    def _add_new_link(self, link, freq):
        """Add a new node to the graph, if not present

        Returns the fingerprint used to add the link
        """
        fingerprint = link.meta['fingerprint']
        self._graph.add_node(fingerprint)
        self._opic.add_page(fingerprint)
        self._pages.add(fingerprint,
                        pagedb.PageData(link.url, link.meta['domain']['name']))
        self._freqs.add(fingerprint, freq)

        return fingerprint

    # FrontierManager interface
    def add_seeds(self, seeds):
        """Start crawling from this seeds"""
        tic = time.clock()

        self._graph.start_batch()
        self._pages.start_batch()

        for seed in seeds:
            self._add_new_link(seed, OpicHitsBackend.DEFAULT_FREQ)

        self._graph.end_batch()
        self._pages.end_batch()

        toc = time.clock()
        self._manager.logger.backend.debug(
            'PROFILE ADD_SEEDS time: {0:.2f}'.format(toc - tic))

    # FrontierManager interface
    def page_crawled(self, response, links):
        """Add page info to the graph and reset its score"""

        tic = time.clock()

        page_fingerprint = response.meta['fingerprint']
        page_h, page_a = self._opic.get_scores(page_fingerprint)

        self._pages.start_batch()
        self._graph.start_batch()
        for link in links:
            # For a new page set its frequency as the hub scores
            # of the parent
            link_fingerprint = self._add_new_link(
                link, page_h)
            self._graph.add_edge(page_fingerprint, link_fingerprint)
        self._graph.end_batch()
        self._pages.end_batch()

        # Set crawl frequency according to authority score
        self._freqs.set(page_fingerprint,
                        page_a/self._opic.a_mean*self.DEFAULT_FREQ)

        # mark page to update
        self._opic.mark_update(page_fingerprint)

        # check page changes and frequency estimation
        if self._freqest and self._pagechange:
            page_status = self._pagechange.update(
                page_fingerprint, response.body)

            if page_status == pagechange.Status.NEW:
                self._freqest.add(page_fingerprint,
                                  OpicHitsBackend.DEFAULT_FREQ)
            else:
                self._freqest.refresh(
                    page_fingerprint, page_status == pagechange.Status.UPDATED)

        toc = time.clock()
        self._manager.logger.backend.debug(
            'PROFILE PAGE_CRAWLED time: {0:.2f}'.format(toc - tic))

    # FrontierManager interface
    def request_error(self, page, error):
        """Remove page from frequency db"""
        self._freqs.delete(page.meta['fingerprint'])

    # Retrieve pages
    ####################################################################
    def _get_request_from_id(self, page_id):
        page_data = self._pages.get(page_id)
        if page_data:
            result = Request(page_data.url)
            result.meta['fingerprint'] = page_id
            if 'domain' not in result.meta:
                result.meta['domain'] = {}
            result.meta['domain']['name'] = page_data.domain
        else:
            result = None

        return result

    # FrontierManager interface
    def get_next_requests(self, max_n_requests):
        """Retrieve the next pages to be crawled"""
        tic = time.clock()

        h_updated, a_updated = self._opic.update()

        for page_id in a_updated:
            h_score, a_score = self._opic.get_scores(page_id)
            self._freqs.set(page_id,
                            a_score/self._opic.a_mean*self.DEFAULT_FREQ)

        # build requests for the best scores, which must be strictly positive
        next_pages = self._freqs.get_next_pages(max_n_requests)
        next_requests = map(self._get_request_from_id, next_pages)

        toc = time.clock()
        self._manager.logger.backend.debug(
            'PROFILE GET_NEXT_REQUESTS time: {0:.2f}'.format(toc - tic))

        return next_requests

    # Just for testing/debugging
    ####################################################################
    def h_scores(self):
        if self._scores._closed:
            return self._h_scores
        else:
            return {self._pages.get(page_id).url: h_score
                    for page_id, h_score, a_score in self._opic.iscores()}

    def a_scores(self):
        if self._scores._closed:
            return self._a_scores
        else:
            return {self._pages.get(page_id).url: a_score
                    for page_id, h_score, a_score in self._opic.iscores()}
