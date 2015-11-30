# -*- coding: utf-8 -*-
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

    def page_crawled(self, response, links):
        self._set_canonical(response)
        for link in links:
            self._set_canonical(link)

    def request_error(self, page, error):
        self._set_canonical(page)

    def _set_canonical(self, obj):
        if 'redirect_urls' in obj.meta:
            redirect_urls = obj.meta['redirect_urls']
            redirect_fingerprints = obj.meta['redirect_fingerprints']
            redirect_urls.append(obj.url)
            redirect_fingerprints.append(obj.meta['fingerprint'])
            obj._url = redirect_urls[0]
            obj.meta['fingerprint'] = redirect_fingerprints[0]

            if 'redirect_domains' in obj.meta:
                redirect_domains = obj.meta['redirect_domains']
                redirect_domains.append(obj.meta['domain'])
                obj.meta['domain'] = redirect_domains[0]