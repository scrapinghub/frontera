from urlparse import urlparse
from collections import deque


class OverusedKeys(list):
    def __init__(self, type='domain'):
        super(OverusedKeys, self).__init__()
        self.type = type


class OverusedBuffer(object):
    def __init__(self, _get_func, log_func=None):
        self._pending = dict()
        self._get = _get_func
        self._log = log_func

    def _get_key(self, request, type):
        key = urlparse(request.url).hostname or ''
        if type == 'ip':
            raise NotImplementedError
        return key

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

    def get_next_requests(self, max_n_requests, overused_keys):
        if self._log:
            self._log("Overused keys: %s" % str(overused_keys))
            self._log("Pending: %i" % (sum([len(pending) for pending in self._pending.itervalues()])))

        overused_set = set(overused_keys)
        requests = self._get_pending(max_n_requests, overused_set)

        if len(requests) == max_n_requests:
            return requests

        for request in self._get(max_n_requests-len(requests), overused_keys):
            key = self._get_key(request, overused_keys.type)
            if key in overused_set:
                self._pending.setdefault(key, deque()).append(request)
            else:
                requests.append(request)
        return requests