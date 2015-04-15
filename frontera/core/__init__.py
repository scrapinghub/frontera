from urlparse import urlparse
from socket import getaddrinfo
from collections import deque


def get_slot_key(request, type):  # TODO: Probably use caching here
    """
    Get string representing a downloader slot key, which will be used in downloader as id for domain/ip load
    statistics and in backend for distinguishing free and overloaded resources. This method used in all Frontera
    backends.

    :param object request: is the instance of :class:`Request <frontera.core.models.Request>`.
    :param str type: either 'domain'(default) or 'ip'.
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
    A buffering object for implementing the buffer of Frontera requests for overused domains/ips. It can be used
    when customizing backend to address efficient downloader pool usage.
    """
    def __init__(self, _get_func, log_func=None):
        """
        :param _get_func: reference to get_next_requests() method of binded class
        :param log_func: optional logging function, for logging of internal state
        """
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

    def get_next_requests(self, max_n_requests, **kwargs):
        if self._log:
            self._log("Overused keys: %s" % str(kwargs['overused_keys']))
            self._log("Pending: %i" % (sum([len(pending) for pending in self._pending.itervalues()])))

        overused_set = set(kwargs['overused_keys'])
        requests = self._get_pending(max_n_requests, overused_set)

        if len(requests) == max_n_requests:
            return requests

        for request in self._get(max_n_requests-len(requests), **kwargs):
            key = self._get_key(request, kwargs['key_type'])
            if key in overused_set:
                self._pending.setdefault(key, deque()).append(request)
            else:
                requests.append(request)
        return requests