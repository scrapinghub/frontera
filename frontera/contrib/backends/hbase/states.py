from __future__ import absolute_import

import logging

from binascii import hexlify, unhexlify
from collections import Iterable
from struct import unpack

from frontera.core.components import States
from frontera.utils.misc import chunks

from .utils import prepare_hbase_object


class HBaseState(States):

    def __init__(self, connection, table_name, cache_size_limit):
        self.connection = connection
        self._table_name = table_name
        self.logger = logging.getLogger("hbase.states")
        self._state_cache = {}
        self._cache_size_limit = cache_size_limit

    def update_cache(self, objs):
        objs = objs if isinstance(objs, Iterable) else [objs]

        def put(obj):
            self._state_cache[obj.meta[b'fingerprint']] = obj.meta[b'state']

        [put(obj) for obj in objs]

    def set_states(self, objs):
        objs = objs if isinstance(objs, Iterable) else [objs]

        def get(obj):
            fprint = obj.meta[b'fingerprint']

            if fprint in self._state_cache:
                obj.meta[b'state'] = self._state_cache[fprint]
            else:
                obj.meta[b'state'] = States.DEFAULT

        [get(obj) for obj in objs]

    def flush(self, force_clear):
        if len(self._state_cache) > self._cache_size_limit:
            force_clear = True

        table = self.connection.table(self._table_name)

        for chunk in chunks(list(self._state_cache.items()), 32768):
            with table.batch(transaction=True) as b:
                for fprint, state in chunk:
                    hb_obj = prepare_hbase_object(state=state)

                    b.put(unhexlify(fprint), hb_obj)

        if force_clear:
            self.logger.debug(
                "Cache has %d requests, clearing", len(self._state_cache),
            )
            self._state_cache.clear()

    def fetch(self, fingerprints):
        to_fetch = [f for f in fingerprints if f not in self._state_cache]

        self.logger.debug("cache size %s", len(self._state_cache))
        self.logger.debug("to fetch %d from %d", len(to_fetch), len(fingerprints))

        for chunk in chunks(to_fetch, 65536):
            keys = [unhexlify(fprint) for fprint in chunk]
            table = self.connection.table(self._table_name)
            records = table.rows(keys, columns=[b's:state'])

            for key, cells in records:
                if b's:state' in cells:
                    state = unpack('>B', cells[b's:state'])[0]
                    self._state_cache[hexlify(key)] = state
