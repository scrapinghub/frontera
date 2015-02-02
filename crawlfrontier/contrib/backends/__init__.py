from crawlfrontier.utils.url import urlparse_cached


class OverusedBuffer(object):
    def __init__(self, _get_func, log_func=None):
        self._pending_requests = set()
        self._get = _get_func
        self._log = log_func

    def get_next_requests(self, max_n_requests, overused_keys):
        if self._log:
            self._log("Overused keys: %s" % str(overused_keys))
            self._log("Pending: %i" % len(self._pending_requests))
        requests = []
        _remove = []
        for request in self._pending_requests:
            hostname = urlparse_cached(request).hostname or ''  # FIXME: ip concurrency support
            if hostname not in overused_keys:
                requests.append(request)
                _remove.append(request)
            if len(requests) == max_n_requests:
                break

        for r in _remove:
            self._pending_requests.remove(r)

        if len(requests) == max_n_requests:
            return requests

        for request in self._get(max_n_requests-len(requests), overused_keys):
            hostname = urlparse_cached(request).hostname or ''  # FIXME: ip concurrency support
            if hostname in overused_keys:
                self._pending_requests.add(request)
            else:
                requests.append(request)
        return requests