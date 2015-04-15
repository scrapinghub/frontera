import re
from time import time

from grequests import AsyncRequest, get as grequests_get, map as grequests_map

from urlparse import urljoin

from frontera.core.models import Request as FrontierRequest
from frontera.utils.converters import BaseRequestConverter
from frontera.contrib.requests.converters import ResponseConverter

from frontera.utils.managers import FrontierManagerWrapper
from frontera.core import get_slot_key
from frontera import Settings

SETTINGS = Settings()
SETTINGS.BACKEND = 'frontera.contrib.backends.memory.MemoryRandomOverusedBackend'
SETTINGS.LOGGING_MANAGER_ENABLED = True
SETTINGS.LOGGING_BACKEND_ENABLED = False
SETTINGS.MAX_REQUESTS = 0
SETTINGS.MAX_NEXT_REQUESTS = 40

SEEDS = [
    'http://www.imdb.com',
    'http://www.bbc.com/',
    'http://www.amazon.com/'
]

LINK_RE = re.compile(r'href="(.*?)"')


class GRequestsConverter(BaseRequestConverter):
    """Converts between frontera and grequests request objects"""
    @classmethod
    def to_frontier(cls, request):
        """request: AsyncRequest > Frontier"""
        return FrontierRequest(url=request.url,
                               method=request.method)

    @classmethod
    def from_frontier(cls, request):
        """request: Frontier > AsyncRequest"""
        return AsyncRequest(method=request.method, url=request.url)


class GRequestsFrontierManager(FrontierManagerWrapper):
    request_converter_class = GRequestsConverter
    response_converter_class = ResponseConverter


class HostnameStatistics(object):
    def __init__(self):
        self.stats = {}

    def on_request(self, request):
        key = get_slot_key(request, 'domain')
        self.stats[key] = time()

    def collect_overused_keys(self, overused):
        ts = time()
        for key, timestamp in self.stats.iteritems():
            if ts - timestamp < 5.0:  # querying each hostname with at least 5 seconds delay
                overused.append(key)
        return overused


def extract_page_links(response):
    return [urljoin(response.url, link) for link in LINK_RE.findall(response.text)]


"""
The idea is to send requests to each domain with at least 5 seconds of delay. grequests only allows us to limit the
number of simultaneous requests. So, we basically performing checks every frontier iteration and limiting the contents
of new frontier batch by sending overused keys in `info` argument to get_next_requests. Therefore, we're getting to 5
seconds delays per batch.
"""


if __name__ == '__main__':

    frontier = GRequestsFrontierManager(SETTINGS)
    stats = HostnameStatistics()
    frontier.add_seeds([grequests_get(url=url.strip()) for url in SEEDS])
    while True:
        def error_handler(request, exception):
            frontier.request_error(request, str(exception))

        def callback(response, **kwargs):
            stats.on_request(response.request)
            links = [grequests_get(url=url) for url in extract_page_links(response)]
            frontier.page_crawled(response=response, links=links)

        dl_info = {'type': 'domain'}
        stats.collect_overused_keys(dl_info['overused_keys'])
        next_requests = frontier.get_next_requests(key_type=dl_info['type'], overused_keys=dl_info['overused_keys'])
        if not next_requests:
            continue

        for r in next_requests:
            r.kwargs['hooks'] = {'response': callback}

        grequests_map(next_requests, size=10)