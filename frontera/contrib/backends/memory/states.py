from __future__ import absolute_import

import logging

from collections import Iterable

from frontera.core.components import States


logger = logging.getLogger(__name__)


class MemoryStates(States):

    def __init__(self, cache_size_limit):
        self._cache = dict()
        self._cache_size_limit = cache_size_limit

    def _put(self, obj):
        self._cache[obj.meta[b'fingerprint']] = obj.meta[b'state']

    def _get(self, obj):
        fprint = obj.meta[b'fingerprint']

        obj.meta[b'state'] = self._cache.get(fprint, States.DEFAULT)

    def update_cache(self, objs):
        objs = objs if isinstance(objs, Iterable) else [objs]

        [self._put(obj) for obj in objs]

    def set_states(self, objs):
        objs = objs if isinstance(objs, Iterable) else [objs]

        [self._get(obj) for obj in objs]

    def fetch(self, fingerprints):
        pass

    def flush(self, force_clear=False):
        if len(self._cache) > self._cache_size_limit:
            force_clear = True

        if force_clear:
            logger.debug("Cache has %d items, clearing", len(self._cache))
            self._cache.clear()
