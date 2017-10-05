# -*- coding: utf-8 -*-
from __future__ import absolute_import
from frontera.core.components import CanonicalSolver


class BasicCanonicalSolver(CanonicalSolver):
    """
    Implements a simple CanonicalSolver taking always first URL from redirect chain, if there were redirects.
    It allows easily to avoid leaking of requests in Frontera (e.g. when request issued by
    :attr:`get_next_requests() <frontera.core.manager.FrontierManager.get_next_requests>` never matched in
    :attr:`page_crawled() <frontera.core.manager.FrontierManager.page_crawled>`) at the price of duplicating
    records in Frontera for pages having more than one URL or complex redirects chains.
    """
    def frontier_start(self):
        pass

    def frontier_stop(self):
        pass

    def add_seeds(self, seeds):
        for seed in seeds:
            self._set_canonical(seed)

    def page_crawled(self, response):
        self._set_canonical(response)

    def links_extracted(self, request, links):
        for link in links:
            self._set_canonical(link)

    def request_error(self, page, error):
        self._set_canonical(page)

    def create_request(self, request):
        self._set_canonical(request)

    def _set_canonical(self, obj):
        if b'redirect_urls' in obj.meta:
            redirect_urls = obj.meta[b'redirect_urls']
            redirect_fingerprints = obj.meta[b'redirect_fingerprints']
            redirect_urls.append(obj.url)
            redirect_fingerprints.append(obj.meta[b'fingerprint'])
            obj._url = redirect_urls[0]
            obj.meta[b'fingerprint'] = redirect_fingerprints[0]

            if b'redirect_domains' in obj.meta:
                redirect_domains = obj.meta[b'redirect_domains']
                redirect_domains.append(obj.meta[b'domain'])
                obj.meta[b'domain'] = redirect_domains[0]
