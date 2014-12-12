'''crawlfrontier.contrib.backends.opic.discovery.OpicHubClassifier is
a Backend implementation that combines three algorithms:

 * Online Page Importance Calculation (OPIC)

 * Expontential backoff/stepup based on page change frequency

 * Classifier for ranking hubs by the content to which they link


Design requirements:

 * Run indefinitely without tuning 
    --> expire URLs from the DB efficiently
    --> cope with not visiting most of the URLs ever, not even once

 * Discover hub pages that link to "good" content
    --> provide a way of defining "good"
    --> revisit hub pages

 * Provide principled modeling framework that allows tuning


Implementation Considerations:

 * revisit_interval is a crucial parameter both in modeling *and*
   storage, because the NEXT_FETCH_TABLE groups on host_hash and
   orders on the `next fetch time` measured in epoch seconds.  Setting
   a next fetch time way in the future will push a page toward
   expiration.

 * get_pages receives a host_hash (or range of them) and scans that
   portion of NEXT_FETCH_TABLE starting with epoch seconds=0 and
   scanning up to `now`.  As it scans, it builds up a size-limited
   priority queue sorted on page importance.

 * OPIC provides both a simple mechanism of PageRank-like page
   importance and also a framework for executing a classifier.  For
   stability, OPIC uses a SPECIAL_PAGE that links to all pages.  That
   page starts with all the cash and acts as a bank.  When the
   Classifier identifies a good page, that page gets a `bonus` of
   extra cash from the SPECIAL_PAGE, which it then disburses to its
   inlinking and outlinking pages.  Pages identified as "not good" by
   the Classifier get a negative bonus and give cash back to the
   SPECIAL_PAGE.

 * low-importance pages will not get prioritized, so their `next fetch
   time` will become farther and farther behind the present.

 * cleanup processes should sweep the database expiring pages with
   either far in the future or far in the past `next fetch times.`

 * a key tuning parameter that needs more thinking is the granularity
   of `next fetch time` values for pages that have never been fetched.
   By setting this to a large window, such as a whole week, we can
   have many separate discoveries of the same page not create
   conflicting records.  The current implementation doesn't quite get
   this right, and maybe it would be better to rely on the
   MetadataRecord to ensure that the NEXT_FETCH_TABLE has only one
   record per URL.


http://www2003.org/cdrom/papers/refereed/p007/p7-abiteboul.html#foot170

.. This software is released under an 3-Clause BSD open source license.
   Copyright 2014 Diffeo, Inc.  ## TODO: setup contributor agreements

'''
from __future__ import division, absolute_import
import copy
import datetime
import heapq
from itertools import islice, imap
import json
from operator import itemgetter
import random
import time

import kvlayer
import mmh3
from nilsimsa import Nilsimsa
from urlparse import urlparse

from crawlfrontier import Backend
from crawlfrontier.core.models import Page as FrontierPage

class MetadataRecord(FrontierPage):
    def __init__(self, *args, **kwargs):
        super(MetadataRecord, self).__init__(*args, **kwargs)
        ## TODO: discuss how to extend this class
        self.revisit_interval = None
        ## TODO: discuss best approach here
        self.host = urlparse(self.url).netloc
        self.host_hash = mmh3.hash_bytes(self.host)[:4]
        self.proba = None
        self.lsh_hexdigest = None
        self.num_new_links = None

    def serialize(self):
        data = dict(
            url=self.url,
            state=self.state,
            depth=self.depth,
            created_at=self.created_at,
            last_update=self.last_update,
            status=self.status,
            n_adds=self.n_adds,
            n_queued=self.n_queued,
            n_crawls=self.n_crawls,
            n_errors=self.n_errors,

            ## added these to FrontierPage
            host_hash=self.host_hash,
            revisit_interval=self.revisit_interval,
            proba=self.proba,
            lsh_hexdigest=self.lsh_hexdigest,
            num_new_links=self.num_new_links,
            )
        ## TODO: discuss more compact serialization, e.g. ordered list of values
        return json.dumps(data)

    @staticmethod
    def deserialize(data):
        data = json.loads(data)
        p = MetadataRecord(data['url'])
        for k, v in data.items():
            setattr(p, k, v)
        return p

class Classifier(object):
    '''example instance of classifier
    '''
    def score(self, raw_content):
        '''trivial score based on page length in bytes normalized to 100k

        '''
        return min(1.0, len(raw_content) / (100 * 2**10))

class OpicHubClassifier(Backend):
    name = 'OPIC Hub Classifier'

    SPECIAL_PAGE = '0'
    TOTAL_CASH = 2**36
    SEED_CASH = 100

    DEFAULT_REVISIT = 7 * 24 * 3600 ## one week of seconds
    ## with 40 fetches/second, that's ~25M/week

    CASH_TABLE = 'cash'
    META_TABLE = 'meta'
    OUTLINKS = 'outlinks'
    INLINKS  =  'inlinks'
    NEXT_FETCH_TABLE = 'next'

    _kvlayer_schema = {
        ## fingerprint is the key on both
        CASH_TABLE: (str, kvlayer.COUNTER_SENTINEL),
        META_TABLE: (str,),

        ## (fingerprint, fingerprint)
        OUTLINKS: (str, str),
         INLINKS: (str, str),

        ## (host_hash, next_fetch_time, fingerprint)
        NEXT_FETCH_TABLE: (str, int, str),
    }

    def __init__(self, manager):
        self.manager = manager
        self.kvl = kvlayer.client()
        self.kvl.setup_namespace(self._kvlayer_schema)
        ## TODO: how to configure this?
        self.classifier = Classifier()
        self.init_cash()

    def init_cash(self):
        '''OPIC uses SPECIAL_PAGE to connect all pages
        '''
        cash = list(self.kvl.get(self.CASH_TABLE, self.SPECIAL_PAGE))
        if len(cash) == 0:
            self.kvl.put(self.CASH_TABLE, self.SPECIAL_PAGE, self.TOTAL_CASH)
        else:
            assert 0 <= cash[0][1] <= self.TOTAL_CASH            

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def add_seeds(self, links):
        '''seed links get depth set to 0 and are given SEED_CASH by taking it
        from the SPECIAL_PAGE

        '''
        self.manager.logger.backend.debug('ADD_SEEDS n_links=%s' % len(links))
        metas = []
        cash_incs = []
        next_fetches = []
        for link in links:
            meta = self._get_meta(link.fingerprint, link.url, depth=0)
            metas.append(meta)
            
            cash_incs.append( (link.fingerprint, self.SEED_CASH) )

            next_fetches.append( next_fetch(meta) )

        ## maintain exact TOTAL_CASH
        cash_incs.append( (self.SPECIAL_PAGE, -self.SEED_CASH * len(cash_incs)) )            
        self.kvl.put(self.CASH_TABLE, *cash_incs)
        self.kvl.put(self.NEXT_FETCH_TABLE, *next_fetches)

        return metas
                

    def page_crawled(self, page, outlinks):
        self.manager.logger.backend.debug('PAGE_CRAWLED page=%s status=%s links=%s' %
                                          (page, page.status, len(outlinks)))
        ## TODO: how to get the body content?
        proba = self.classifier.score(page.body)

        ## prepare for locality sensitive comparison inside _get_meta
        lsh = Nilsimsa(page.body)

        key_range = ((page.fingerprint,), (page.fingerprint,))
        ## should protect against OOM?
        previous_outlinks = set(list(self.kvl.scan(self.OUTLINKS, key_range)))
        num_new_links = len(set(outlinks) - previous_outlinks)

        meta, created = self._get_meta(
            page.fingerprint, page.url, crawled=True,
            proba=proba,
            lsh=lsh,
            num_new_links=num_new_links,
            num_outlinks=len(outlinks),
        )
        assert not created            
        
        next_fetches = [ next_fetch(meta) ]
        ## TODO: delete the previous record from NEXT_FETCH_TABLE

        depth = page.depth + 1
        for link in outlinks:
            self.manager.logger.backend.debug('ADD_LINK link=%s' % link)
            ## TODO: consider adding proba to MetadataRecord?
            meta, created = self._get_meta(link.fingerprint, link.url, depth=depth)
            if created:
                next_fetches.append( next_fetch(meta, when_delta=0) )

        ## OPIC
        current_cash  = self.kvl.get(self.CASH_TABLE, page.fingerprint)
        bonus = self.SEED_CASH * (2 * proba - 1)  ## \epsilon in OPIC paper
        disbursable = current_cash + bonus
        if disbursable < 0:
            ## give it all back to SPECIAL_PAGE
            bonus = -current_cash
            disbursable = 0

        cash_incs  = [(self.SPECIAL_PAGE, -bonus), (page.fingerprint, -current_cash)]
        cash_incs += [(l.fingerprint, disbursable // (2 * len(outlinks))) for l in outlinks]

        ## should protect against OOM by streaming inlinks from scan to put...
        inlinks = list(self.kvl.scan(self.INLINK_, key_range))
        cash_incs += [(ikv[0][1], disbursable // (2 * len( inlinks))) for ikv in inlinks]

        self.kvl.put(self.CASH_TABLE, *cash_incs)
        self.kvl.put(self.NEXT_FETCH_TABLE, *next_fetches)

        return meta


    def page_crawled_error(self, page, error):
        self.manager.logger.backend.debug('PAGE_CRAWLED_ERROR page=%s error=%s' % (page, error))
        ## put back all of the cash that this page had.
        cash  = self.kvl.get(self.CASH_TABLE, page.fingerprint)
        self.kvl.put(self.CASH_TABLE, *[(self.SPECIAL_PAGE, cash), (page.fingerprint, -cash)])
        return self._get_meta(page.fingerprint, page.url, error=True)[0]

    def get_next_pages(self, max_next_pages):
        self.manager.logger.backend.debug('GET_NEXT_PAGES max_next_pages=%s' % max_next_pages)

        ## TODO: how to get a host or host range assignment?
        fetches = self.kvl.scan(self.NEXT_FETCH_TABLE, ((host_start, 0), (host_end, time.time())))

        ## TODO: how to get config for max_scan_size
        next_links = []
        for host_hash, when, fingerprint in islice(fetches, max_scan_size):
            ## unfortunate to randomly seek here, but is inevitable
            cash = self.kvl.get(self.CASH_TABLE, fingerprint)
            if len(next_links) < batch_size:
                heapq.heappush(next_links, (cash, fingerprint))
                continue
            ## maintain max heap size
            heapq.heapreplace(next_links, (cash, fingerprint))

        next_pages = []
        for cash, fingerprint in next_links:
            next_pages.append(
                self._get_meta(
                    fingerprint, 
                    state=self.manager.page_model.States.QUEUED,
                )[0]
            )

        ## TODO: verify which kind of "Page" object this wants?
        return next_pages


    def get_page(self, link):
        metas = list(self.kvl.get(self.META_TABLE, link.fingerprint))
        return len(metas) > 0 and MetadataRecord(metas[0][1]) or None


    def _get_meta(
            self, fingerprint, url=None, depth=None, now=None, 
            error=False, crawled=False,
            lsh=None, proba=None, num_new_links=None,
            num_outlinks=None,
            state=None,
    ):
        if depth is None:
            raise Exception('must always set link depth from seed')

        changed = False
        if now is None:
            now = datetime.datetime.utcnow()

        metas = list(self.kvl.get(self.META_TABLE, fingerprint))
        if len(metas) == 0:
            meta = MetadataRecord(url)
            meta.fingerprint = fingerprint
            meta.depth = depth
            meta.created_at = now
            lsh_distance = None
            changed = True
            self.manager.logger.backend.debug('Create: %s' % meta)

        else:
            meta = MetadataRecord.deserialize(metas[0][1])
            if meta.depth > depth:
                meta.depth = depth
                changed = True
            lsh_distance = lsh.compare(meta.lsh_hexdigest, ishex=True)
            self.manager.logger.backend.debug('Exists: %s' % meta)

        if state:
            meta.state = state
            changed = True

        ## TODO: what are these for?
        meta.last_update = now
        meta.last_iteration = self.manager.iteration

        meta.proba = proba

        if lsh:
            assert num_outlinks is not None
            assert num_new_links is not None
            meta.lsh_hexdigest = lsh.hexdigest()
            meta.num_new_links = num_new_links
            changed = True

            ## exponential backoff or stepup of revisit_interval
            if lsh_distance < 100 or num_new_links > num_outlinks * 0.1:
                ## no faster than every 15 minutes
                self.revisit_interval = max(900, self.revisit_interval // 2)
            else:
                self.revisit_interval = self.revisit_interval * 2

        if error:
            meta.n_errors += 1
            meta.state = self.manager.page_model.State.ERROR
            changed = True
        
        if crawled:
            meta.n_crawls += 1
            meta.state = self.manager.page_model.State.CRAWLED
            changed = True
        
        if changed:
            ## TODO: optimization: instead of putting this here, let
            ## the caller batch them up and put many at once.
            self.kvl.put(self.META_TABLE, fingerprint, meta.serialize())

        ## returns a "Page" and was_created
        return meta, len(metas)==0


WINDOW_SIZE = 3600 # one hour

def next_fetch(meta, when_delta=None):
    '''links are stored in NEXT_FETCH_TABLE table aggregated at the
    WINDOW_SIZE to reduce duplication in the table from many inlinking
    pages being observed separately and each trying to schedule;

    TODO: figure out detailed model of revisit batch size; WINDOW_SIZE
    of a week might be better.

    TODO: combine with *deleting* URLs that are two far in the future
    or two far in the past, for lack of priority given crawl
    resources.

    '''
    if when_delta is None:
        when_delta = meta.revisit_interval

    now_hours = int(datetime.datetime.utcnow().strftime('%s')) // WINDOW_SIZE
    when = now_hours + when_delta

    return (meta.host_hash, when, meta.fingerprint)
