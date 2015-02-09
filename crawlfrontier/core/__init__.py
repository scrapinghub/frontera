from urlparse import urlparse
from socket import getaddrinfo
from collections import deque


class DownloaderInfo(object):
    """
    Data class, used for carrying various information from downloading component of crawler to Crawl Frontier(CF)
    backend. It's main purpose is to make backend aware of downloader fetching activities.

    Typically the design of URL ordering implies fetching many URLs from the same domain. If crawling process needs to
    be polite it has to preserve some delay and rate of requests. From the other side, there are downloaders which can
    afford downloading many URLs at once, in parallel. So, flooding of the URLs from the same domain leads to
    inefficient waste of downloader connection pool resources.

    Using DownloaderInfo one can implement backend taking into account domain overuse in downloader and thus
    returning URLs from other domains, which aren't overused.

    The DownloaderInfo instance is created outside of Crawl Frontier, and then passed to CF via FrontierManagerWrapper
    subclass to backend.
    """
    def __init__(self, type='domain'):
        self._overused_keys = []
        self._type = type

    @property
    def overused_keys(self):
        """
        Key here could be anything: domain, ip or custom object. It is defined in downloader and should be expected by
        backend.

        :return: A list of keys
        """
        return self._overused_keys

    @property
    def key_type(self):
        return self._type


def get_slot_key(request, type):  # TODO: Probably use caching here
    """
    Get string representing a downloader slot key, which will be used in downloader as id for domain/ip load
    statistics and in backend for distinguishing free and overloaded resources.

    :param request: :class:`Request <crawlfrontier.core.models.Request>`
    :param type: a string 'ip' or 'domain'
    :return: string
    """
    key = urlparse(request.url).hostname or ''
    if type == 'ip':
        for result in getaddrinfo(key, 80):
            key = result[4][0]
            break
    return key


class OverusedBuffer(object):
    """
    A buffering object for implementing the buffer of Crawl Frontier requests for overused domains/ips. It can be used
    when customizing backend to address efficient downloader pool usage.

    Attention! Scrapy users should use :class:`OverusedBufferScrapy <crawlfrontier.contrib.scrapy.overusedbuffer.OverusedBufferScrapy>`
    """
    def __init__(self, _get_func, log_func=None):
        self._pending = dict()
        self._get = _get_func
        self._log = log_func

    def _get_key(self, request, type):
        return get_slot_key(request, type)

    def _get_pending(self, max_n_requests, overused_set):
        requests = []
        trash_can = []
        try:
            while True:
                left = 0
                for key, pending in self._pending.iteritems():
                    if key in overused_set:
                        continue

                    if pending:
                        requests.append(pending.popleft())
                        if not pending:
                            trash_can.append(key)
                        left += len(pending)

                    if len(requests) == max_n_requests:
                        return requests

                if left == 0:
                    return requests
        finally:
            for k in trash_can:
                del self._pending[k]

    def get_next_requests(self, max_n_requests, downloader_info):
        if self._log:
            self._log("Overused keys: %s" % str(downloader_info.overused_keys))
            self._log("Pending: %i" % (sum([len(pending) for pending in self._pending.itervalues()])))

        overused_set = set(downloader_info.overused_keys)
        requests = self._get_pending(max_n_requests, overused_set)

        if len(requests) == max_n_requests:
            return requests

        for request in self._get(max_n_requests-len(requests), downloader_info=downloader_info):
            key = self._get_key(request, downloader_info.key_type)
            if key in overused_set:
                self._pending.setdefault(key, deque()).append(request)
            else:
                requests.append(request)
        return requests