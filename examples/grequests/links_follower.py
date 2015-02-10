import re
from time import time, sleep

from grequests import AsyncRequest, get as grequests_get, map as grequests_map

from urlparse import urljoin

from crawlfrontier.core.models import Request as FrontierRequest
from crawlfrontier.utils.converters import BaseRequestConverter
from crawlfrontier.contrib.requests.converters import ResponseConverter

from crawlfrontier.utils.managers import FrontierManagerWrapper
from crawlfrontier.core import get_slot_key, DownloaderInfo
from crawlfrontier import Settings

SETTINGS = Settings()
SETTINGS.BACKEND = 'crawlfrontier.contrib.backends.memory.MemoryDFSOverusedBackend'
SETTINGS.LOGGING_MANAGER_ENABLED = True
SETTINGS.LOGGING_BACKEND_ENABLED = False
SETTINGS.MAX_REQUESTS = 0
SETTINGS.MAX_NEXT_REQUESTS = 40

SEEDS = [
    'http://www.imdb.com',
]

LINK_RE = re.compile(r'href="(.*?)"')

class GRequestConverter(BaseRequestConverter):
    """Converts between crawlfrontier and grequests request objects"""
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
    request_converter_class = GRequestConverter
    response_converter_class = ResponseConverter


class HostnameStatistics(object):
    def __init__(self):
        self.stats = {}

    def on_request(self, request):
        key = get_slot_key(request, 'domain')
        self.stats[key] = time()

    def get_overused_keys(self):
        overused = []
        ts = time()
        for key, timestamp in self.stats.iteritems():
            if ts - timestamp < 5.0:  # querying each hostname with at least 5 seconds delay
                overused.append(key)
        return overused


def extract_page_links(response):
    return [urljoin(response.url, link) for link in LINK_RE.findall(response.text)]

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

        dl_info = DownloaderInfo()
        dl_info._overused_keys = stats.get_overused_keys()
        next_requests = frontier.get_next_requests(downloader_info=dl_info)
        if not next_requests:
            sleep(5)
            continue

        for r in next_requests:
            r.kwargs['hooks'] = {'response': callback}

        grequests_map(next_requests, size=10)