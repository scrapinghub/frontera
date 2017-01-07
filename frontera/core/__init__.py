from __future__ import absolute_import
from six.moves.urllib.parse import urlparse
from socket import getaddrinfo
from collections import defaultdict, deque
import six


class OverusedBuffer(object):
    """
    A buffering object for implementing the buffer of Frontera requests for
    overused domains/ips. It can be used when customizing backend to address
    efficient downloader pool usage.
    """

    def __init__(self):
        self._pending = defaultdict(deque)

    @property
    def pending(self):
        """
        Number of items in the buffer.
        """

        return sum(six.moves.map(len, six.itervalues(self._pending)))

    def push(self, key, item):
        """
        Add single item to the buffer.

        :param str key: a key
        :param object item: an item
        """

        self._pending[key].append(item)

    def push_multiple(self, key, items):
        """
        Add multiple items to the buffer.

        :param str key: a key
        :param collections.Iterable items: collection of items
        """

        self._pending[key].extend(items)

    def push_mapping(self, mapping):
        """
        Add multiple items to the buffer.

        :param collections.Mapping items: mapping of the item collections
        """

        for key, items in six.iteritems(mapping):
            self._pending[key].extend(items)

    def push_sequence(self, sequence, key_function):
        """
        Add multiple items to the buffer.

        ``key_function(item)`` will be used to calculate key of each item.

        :param collections.Iterable sequence: collection of items
        :param function key_function: key getter function
        """

        for item in sequence:
            self._pending[key_function(item)].append(item)

    def pull(self, n, overused_keys=None):
        """
        Retrieve up to ``n`` items from the buffer.

        :param int n: number of items to retrieve
        :param set overused_keys: overused key set
        :returns: ``n`` items from the buffer
        :rtype: generator
        """

        pending = self._pending
        i, keys = 0, set(pending)

        if overused_keys is not None:
            keys -= set(overused_keys)

        if n < 0:
            n = self.pending

        while i < n and keys:
            for key in keys.copy():
                try:
                    yield pending[key].popleft()
                    i += 1
                except IndexError:
                    keys.discard(key)
                    del pending[key]


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


class LegacyOverusedBuffer(object):
    """
    Wrapper around :class:`.OverusedBuffer` to ease migration in old code.
    """
    def __init__(self, _get_func, log_func=None):
        """
        :param _get_func: reference to get_next_requests() method of binded class
        :param log_func: optional logging function, for logging of internal state
        """
        self._buffer = OverusedBuffer()
        self._get = _get_func
        self._log = log_func

    def _get_key(self, request, type):
        return get_slot_key(request, type)

    def _get_pending(self, max_n_requests, overused_set):
        return list(self._buffer.pull(max_n_requests, overused_set))

    def get_next_requests(self, max_n_requests, **kwargs):
        if self._log:
            self._log("Overused keys: %s" % str(kwargs['overused_keys']))
            self._log("Pending: %d" % self._buffer.pending)

        overused_set = set(kwargs['overused_keys'])
        requests = self._get_pending(max_n_requests, overused_set)

        if len(requests) == max_n_requests:
            return requests

        for request in self._get(max_n_requests-len(requests), **kwargs):
            key = self._get_key(request, kwargs['key_type'])
            if key in overused_set:
                self._buffer.push(key, request)
            else:
                requests.append(request)
        return requests
