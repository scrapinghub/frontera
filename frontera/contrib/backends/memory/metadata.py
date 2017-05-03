from __future__ import absolute_import

from frontera.core.components import Metadata


class MemoryMetadata(Metadata):
    def __init__(self):
        self.requests = {}

    def request_error(self, request, error):
        request.meta[b'error'] = error
        self._get_or_create_request(request)

    def page_crawled(self, response):
        self._get_or_create_request(response.request)

    def links_extracted(self, request, links):
        for link in links:
            self._get_or_create_request(link)

    def add_seeds(self, seeds):
        for seed in seeds:
            self._get_or_create_request(seed)

    def _get_or_create_request(self, request):
        fingerprint = request.meta[b'fingerprint']

        if fingerprint not in self.requests:
            new_request = request.copy()
            self.requests[fingerprint] = new_request

            return new_request, True
        else:
            page = self.requests[fingerprint]

            return page, False

    def update_score(self, batch):
        pass
