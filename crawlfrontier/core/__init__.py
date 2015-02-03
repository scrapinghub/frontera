from urlparse import urlparse

class OverusedKeys(list):
    def __init__(self, type='domain'):
        super(OverusedKeys, self).__init__()
        self.type = type


class OverusedBuffer(object):
    def __init__(self, _get_func, log_func=None):
        self._pending_requests = set()
        self._get = _get_func
        self._log = log_func

    def _get_key(self, request, type):
        key = urlparse(request.url).hostname or ''
        if type == 'ip':
            raise NotImplementedError
        return key

    def get_next_requests(self, max_n_requests, overused_keys):
        if self._log:
            self._log("Overused keys: %s" % str(overused_keys))
            self._log("Pending: %i" % len(self._pending_requests))
        requests = []
        _remove = []
        for request in self._pending_requests:
            key = self._get_key(request, overused_keys.type)
            if key not in overused_keys:
                requests.append(request)
                _remove.append(request)
            if len(requests) == max_n_requests:
                break

        for r in _remove:
            self._pending_requests.remove(r)

        if len(requests) == max_n_requests:
            return requests

        for request in self._get(max_n_requests-len(requests), overused_keys):
            key = self._get_key(request, overused_keys.type)
            if key in overused_keys:
                self._pending_requests.add(request)
            else:
                requests.append(request)
        return requests