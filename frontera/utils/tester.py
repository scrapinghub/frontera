from __future__ import absolute_import, print_function

from collections import OrderedDict, deque

import six

from io import BytesIO
from os import linesep
from six.moves import range
from six.moves.urllib.parse import urlparse


class FrontierTester(object):

    def __init__(self, frontier, graph_manager, downloader_simulator, max_next_requests=0):
        self.frontier = frontier
        self.graph_manager = graph_manager
        self.max_next_requests = max_next_requests
        self.sequence = []
        self.downloader_simulator = downloader_simulator

    def run(self, add_all_pages=False):
        if not self.frontier.auto_start:
            self.frontier.start()
        if not add_all_pages:
            self._add_seeds()
        else:
            self._add_all()
        while True:
            result = self._run_iteration()
            self.sequence.append(result)
            requests, iteration, dl_info = result
            if not requests and self.downloader_simulator.idle():
                break
        self.frontier.stop()

    def _add_seeds(self):
        stream = BytesIO()
        for seed in self.graph_manager.seeds:
            stream.write(seed.url.encode('utf8'))
            stream.write(linesep.encode('utf8'))
        stream.seek(0)
        self.frontier.add_seeds(stream)

    def _add_all(self):
        stream = BytesIO()
        for page in self.graph_manager.pages:
            stream.write(page.url.encode('utf8'))
            if not page.has_errors:
                for link in page.links:
                    stream.write(link.url.encode('utf8'))
                    stream.write(linesep.encode('utf8'))
        stream.seek(0)

        self.frontier.add_seeds(stream)

    def _make_request(self, url):
        r = self.frontier.request_model(url=url,
                                        headers={
                                            b'X-Important-Header': b'Frontera'
                                        },
                                        method=b'POST',
                                        cookies={b'currency': b'USD'})
        r.meta[b'this_param'] = b'should be passed over'
        return r

    def _make_response(self, url, status_code, request):
        return self.frontier.response_model(url=url, status_code=status_code, request=request)

    def _run_iteration(self):
        kwargs = self.downloader_simulator.downloader_info()
        if self.max_next_requests:
            kwargs['max_next_requests'] = self.max_next_requests

        requests = self.frontier.get_next_requests(**kwargs)

        self.downloader_simulator.update(requests)

        for page_to_crawl in self.downloader_simulator.download():
            crawled_page = self.graph_manager.get_page(url=page_to_crawl.url)
            if not crawled_page.has_errors:
                response = self._make_response(url=page_to_crawl.url,
                                               status_code=crawled_page.status,
                                               request=page_to_crawl)
                self.frontier.page_crawled(response=response)
                self.frontier.links_extracted(request=response.request,
                                              links=[self._make_request(link.url) for link in crawled_page.links])
            else:
                self.frontier.request_error(request=page_to_crawl,
                                            error=crawled_page.status)
            assert page_to_crawl.meta[b'this_param'] == b'should be passed over'
            assert page_to_crawl.headers[b'X-Important-Header'] == b'Frontera'
            assert page_to_crawl.method == b'POST'
            assert page_to_crawl.cookies[b'currency'] == b'USD'
        return (requests, self.frontier.iteration, kwargs)


class BaseDownloaderSimulator(object):
    def __init__(self):
        self.requests = None

    def update(self, requests):
        self.requests = requests

    def download(self):
        return self.requests

    def downloader_info(self):
        return {
            'key_type': 'domain',
            'overused_keys': []
        }

    def idle(self):
        return True


class DownloaderSimulator(BaseDownloaderSimulator):
    def __init__(self, rate):
        self._requests_per_slot = rate
        self.slots = OrderedDict()
        super(DownloaderSimulator, self).__init__()

    def update(self, requests):
        for request in requests:
            hostname = urlparse(request.url).hostname or ''
            self.slots.setdefault(hostname, deque()).append(request)

    def download(self):
        output = []
        _trash_can = []
        for key, requests in six.iteritems(self.slots):
            for i in range(min(len(requests), self._requests_per_slot)):
                output.append(requests.popleft())
            if not requests:
                _trash_can.append(key)

        for key in _trash_can:
            del self.slots[key]
        return output

    def downloader_info(self):
        info = {
            'key_type': 'domain',
            'overused_keys': []
        }
        for key, requests in six.iteritems(self.slots):
            if len(requests) > self._requests_per_slot:
                info['overused_keys'].append(key)
        return info

    def idle(self):
        return len(self.slots) == 0


