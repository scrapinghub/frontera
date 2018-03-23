from __future__ import absolute_import
from six.moves.urllib.parse import urlparse
from socket import getaddrinfo
from collections import defaultdict, deque
from logging import getLogger, DEBUG
from random import sample
import six


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
    def __init__(self, _get_func, max_per_key, keep_per_key, max_keys, keep_keys):
        """
        :param _get_func: reference to get_next_requests() method of binded class
        """
        self._pending = defaultdict(deque)
        self._get = _get_func
        self._log = getLogger("overusedbuffer")
        self._max_per_key = max_per_key
        self._keep_per_key = keep_per_key
        self._max_keys = max_keys
        self._keep_keys = keep_keys

    def _get_key(self, request, type):
        return get_slot_key(request, type)

    def _get_pending_count(self):
        return sum(six.moves.map(len, six.itervalues(self._pending)))

    def _get_key_count(self):
        return len(self._pending)

    def _get_pending(self, max_n_requests, overused_set):
        pending = self._pending
        i, keys = 0, set(pending) - overused_set

        while i < max_n_requests and keys:
            for key in keys.copy():
                try:
                    yield pending[key].popleft()
                    self._check_and_purge(key)
                    i += 1
                except IndexError:
                    keys.discard(key)
                    del pending[key]

    def _check_and_purge(self, key):
        pending = self._pending[key]
        if self._max_per_key is not None and len(pending) > self._max_per_key:
            self._log.warning("Purging of key %s, of size %d has started", key,
                              len(pending))
            purged = 0
            while len(pending) > self._keep_per_key:
                pending.popleft()
                purged += 1
            self._log.warning("%d requests purged", purged)

    def _check_and_purge_keys(self):
        if self._max_keys is not None and len(self._pending) > self._max_keys:
            self._log.warning("Purging the keys")
            new_keys = set(sample(self._pending.keys(), self._keep_keys))
            keys = set(self._pending.keys())
            while keys:
                key = keys.pop()
                if key not in new_keys:
                    del self._pending[key]
            self._log.warning("Finished purging of keys")

    def get_next_requests(self, max_n_requests, **kwargs):
        if self._log.isEnabledFor(DEBUG):
            self._log.debug("Overused keys: %s", str(kwargs['overused_keys']))
            self._log.debug("Pending: %i", self._get_pending_count())
        self._check_and_purge_keys()
        overused_set = set(kwargs['overused_keys'])
        requests = list(self._get_pending(max_n_requests, overused_set))

        if len(requests) == max_n_requests:
            return requests

        for request in self._get(max_n_requests-len(requests), **kwargs):
            key = self._get_key(request, kwargs['key_type'])
            if key in overused_set:
                self._pending[key].append(request)
                # contacts-crawler strategy related hack
                if self._max_per_key:
                    self._check_and_purge(key)
            else:
                requests.append(request)
        return requests
