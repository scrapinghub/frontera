from __future__ import absolute_import

import logging
from collections import defaultdict
from time import time

import six
from msgpack import packb, unpackb
from w3lib.util import to_bytes, to_native_str

from frontera.core.components import DomainMetadata
from frontera.contrib.backends.hbase.utils import HardenedBatch
from frontera.utils.msgpack import restruct_for_pack

import collections
from cachetools import Cache


DEFAULT_HBASE_THRIFT_FRAME_SIZE = 2097152


class LRUCache(Cache):
    """Least Recently Used (LRU) cache implementation."""

    def __init__(self, maxsize, missing=None, getsizeof=None):
        Cache.__init__(self, maxsize, missing, getsizeof)
        self.__order = collections.OrderedDict()

    def __getitem__(self, key, cache_getitem=Cache.__getitem__):
        value = cache_getitem(self, key)
        self._update_order(key)
        return value

    def __setitem__(self, key, value, cache_setitem=Cache.__setitem__):
        cache_setitem(self, key, value)
        self._update_order(key)

    def __delitem__(self, key, cache_delitem=Cache.__delitem__):
        cache_delitem(self, key)
        del self.__order[key]

    def popitem(self):
        """Remove and return the `(key, value)` pair least recently used."""
        try:
            key = next(iter(self.__order))
        except StopIteration:
            raise KeyError('%s is empty' % self.__class__.__name__)
        else:
            return (key, self.pop(key))

    if hasattr(collections.OrderedDict, 'move_to_end'):
        def _update_order(self, key):
            try:
                self.__order.move_to_end(key)
            except KeyError:
                self.__order[key] = None
    else:
        def _update_order(self, key):
            try:
                self.__order[key] = self.__order.pop(key)
            except KeyError:
                self.__order[key] = None


class DomainCache(LRUCache, DomainMetadata):
    """
    This is an implementation of Domain metadata cache backed by HBase table. It's main purpose is to store the domain
    metadata in Python-friendly structures while providing fast and reliable access.
    The container has these features:
        * LRU logic,
        * two generations, second generation is used for evicted items when HBase batch isn't full,
        * batched HBase writes,
        * Python 3 and PyPy ready.

    This container has these limitations:
        1. value is always of dict type
        2. data in value cannot be bigger than MAX_VALUE_SIZE (which is usually ~2Mb), Otherwise fields will be dropped
        with error message
        3. 255 > len(key) > 0
        4. key and keys within value dict are always of native string type
        5. all keys are utf-8 strings.
        6. iterator of this container iterates only on first generation content.
    """

    MAX_VALUE_SIZE = int(DEFAULT_HBASE_THRIFT_FRAME_SIZE * 0.95)
    LOG_INTERVAL = 60.0

    def __init__(self, maxsize, connection, table_name, set_fields=None, on_get_func=None, batch_size=100):
        super(DomainCache, self).__init__(maxsize)

        self._second_gen = dict()

        table_name = to_bytes(table_name)
        self._table = self._get_domain_table(connection, table_name)
        self._batch = HardenedBatch(self._table, batch_size=batch_size)
        self._set_fields = set(set_fields) if set_fields else set()
        self._on_get_func = on_get_func

        self.logger = logging.getLogger("domain-cache")
        self.stats = defaultdict(int)
        self.next_log = time() + self.LOG_INTERVAL
        self.batch_size = batch_size

    # Primary methods

    def __setitem__(self, key, value):
        self._key_check(key)
        assert isinstance(value, dict)
        super(DomainCache, self).__setitem__(key, value)

    def __getitem__(self, key):
        self._key_check(key)
        try:
            value = Cache.__getitem__(self, key)
        except KeyError:
            try:
                value = self._second_gen[key]
            except KeyError:
                try:
                    value = self._get_item(key)
                except KeyError as ke3:
                    raise ke3
                else:
                    self.__setitem__(key, value)
            else:
                self.__setitem__(key, value)
                if key in self._second_gen:   # the second gen clean up could be triggered during set in first gen
                    del self._second_gen[key]
        else:
            self._update_order(key)
        return value

    def __delitem__(self, key):
        self._key_check(key)
        not_found = True
        if super(DomainCache, self).__contains__(key):
            super(DomainCache, self).__delitem__(key)
            not_found = False
        if key in self._second_gen:
            del self._second_gen[key]
            not_found = False
        rk = to_bytes(key)
        if self._table.row(rk):
            self._table.delete(rk)
            not_found = False
        if not_found:
            raise KeyError

    def __contains__(self, key):
        self._key_check(key)
        self.stats["contains"] += 1
        if super(DomainCache, self).__contains__(key):
            self.stats["contains_in_memory"] += 1
            return True
        if key in self._second_gen:
            self.stats["contains_in_secgen"] += 1
            return True
        if self._table.row(to_bytes(key)):
            self.stats["contains_in_hbase"] += 1
            return True
        self.stats["contains_false"] += 1
        return False

    def popitem(self):
        """
        Called every time item is evicted by LRU cache
        """
        key, value = super(DomainCache, self).popitem()
        self._second_gen[key] = value
        self.stats["pops"] += 1
        if len(self._second_gen) >= self.batch_size:
            self._flush_second_gen()
            self._second_gen.clear()
            self.stats["flushes"] += 1

    # These methods aren't meant to be implemented

    def __missing__(self, key):
        raise KeyError

    __len__ = None

    clear = None

    maxsize = None

    # Secondary methods, those that are depend on primary

    def get(self, key, default=None):
        """
        HBase-optimized get
        """
        self._key_check(key)
        self._log_and_rotate_stats()
        if super(DomainCache, self).__contains__(key) or key in self._second_gen:
            self.stats["gets_memory_hit"] += 1
            return self[key]
        try:
            value = self._get_item(key)
        except KeyError:
            self.stats["gets_miss"] += 1
            return default
        else:
            self.stats["gets_hbase_hit"] += 1
            return value

    def setdefault(self, key, default=None):
        """
        HBase-optimized setdefault
        """
        self._key_check(key)
        self.stats["gets"] += 1
        self._log_and_rotate_stats()
        if super(DomainCache, self).__contains__(key) or key in self._second_gen:
            value = self[key]
            self.stats["gets_memory_hit"] += 1
        else:
            try:
                value = self._get_item(key)
            except KeyError:
                self.stats["gets_miss"] += 1
                value = default
            else:
                self.stats["gets_hbase_hit"] += 1
            self[key] = value
        return value

    def flush(self):
        for k, v in six.iteritems(self):
            try:
                self._store_item_batch(k, v)
            except Exception:
                self.logger.exception("Error storing kv pair %s, %s", k, v)
                pass
        self._flush_second_gen()
        self._batch.send()

    # private

    def _flush_second_gen(self):
        for key, value in six.iteritems(self._second_gen):
            self._store_item_batch(key, value)
        self._batch.send()

    def _log_and_rotate_stats(self):
        if not self.logger.isEnabledFor(logging.DEBUG):
            return
        if time() > self.next_log:
            for k, v in self.stats.items():
                self.logger.debug("%s = %d", k, v)
            self.next_log = time() + self.LOG_INTERVAL
            self.stats = defaultdict(int)

    def _get_domain_table(self, connection, table_name):
        tables = set(connection.tables())
        if table_name not in tables:
            schema = {'m': {'max_versions': 1}}
            connection.create_table(table_name, schema)
        return connection.table(table_name)

    def _get_item(self, key):
        self.stats["hbase_gets"] += 1
        hbase_key = to_bytes(key)
        row = self._table.row(hbase_key)
        if not row:
            self.stats["hbase_misses"] += 1
            super(DomainCache, self).__missing__(key)
            raise KeyError
        value = {}
        for k, v in six.iteritems(row):
            cf, _, col = k.partition(b':')
            col = to_native_str(col)
            value[col] = unpackb(v, encoding='utf-8')
            # XXX extract some fields as a set for faster in-checks
            if col in self._set_fields:
                value[col] = set(value[col])
        if self._on_get_func:
            self._on_get_func(value)
        return value

    def _store_item_batch(self, key, value):
        data = {}
        self._key_check(key)
        for k, v in six.iteritems(value):
            if k.startswith('_'):
                continue
            # convert set to list manually for successful serialization
            v = restruct_for_pack(v)
            k = to_bytes(k)
            data[b"m:%s" % k] = packb(v, use_bin_type=True)
        tries = 3
        while data and tries > 0:
            try:
                self._batch.put(key, data)
            except ValueError:
                self.logger.exception("Exception happened during item storing, %d tries left", tries)
                data_lengths = dict((k, len(v)) for k, v in six.iteritems(data))
                self.logger.info("RK %s per-column lengths %s", key, str(data_lengths))
                for k, length in data_lengths.items():
                    if length > self.MAX_VALUE_SIZE:
                        self.logger.info("Dropping key %s", k)
                        del data[k]
                tries -= 1
                continue
            else:
                break

    def _key_check(self, key):
        if len(key) == 0 or len(key) > 255:
            raise KeyError("Key cannot be empty or longer than 255 chars")