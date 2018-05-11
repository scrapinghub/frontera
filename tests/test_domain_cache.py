# -*- coding: utf-8 -*-
from frontera.contrib.backends.hbase.domaincache import DomainCache
from happybase import Connection
import logging
import unittest


class TestDomainCache(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        self.conn = Connection(host="hbase-docker")
        if b'domain_metadata' not in self.conn.tables():
            self.conn.create_table('domain_metadata', {
                'm': {'max_versions': 1, 'block_cache_enabled': 1,}
            })
        t = self.conn.table('domain_metadata')
        t.delete('d1')
        t.delete('d2')
        t.delete('d3')
        t.delete('d4')

    def test_domain_cache_both_generations(self):
        dc = DomainCache(2, self.conn, 'domain_metadata')
        dc['d1'] = {'domain': 1}
        dc['d2'] = {'domain': 2}

        # eviction should happen
        dc['d3'] = {'domain': [3, 2, 1]}
        dc['d4'] = {'domain': 4}

        assert dc['d1'] == {'domain': 1}
        assert dc['d2'] == {'domain': 2}
        assert dc['d3'] == {'domain': [3, 2, 1]}
        assert dc['d4'] == {'domain': 4}

    def test_domain_cache_get_with_default(self):
        dc = DomainCache(2, self.conn, 'domain_metadata')
        dc['d1'] = {'domain': 1}
        dc['d2'] = {'domain': 2}
        dc['d3'] = {'domain': [3, 2, 1]}
        dc['d4'] = {'domain': 4}

        assert dc.get('d1', {}) == {'domain': 1}
        assert dc.get('d3', {}) == {'domain': [3, 2, 1]}

    def test_domain_cache_setdefault(self):
        dc = DomainCache(2, self.conn, 'domain_metadata')
        dc['d1'] = {'domain': 1}
        dc['d2'] = {'domain': 2}
        dc['d3'] = {'domain': [3, 2, 1]}
        dc['d4'] = {'domain': 4}

        assert dc.setdefault('d1', {}) == {'domain': 1}
        assert dc.setdefault('d5', {'domain': 6}) == {'domain': 6}
        dc.flush()
        assert dc.setdefault('d3', {}) == {'domain': [3, 2, 1]}

    def test_domain_cache_setdefault_with_second_gen_flush(self):
        dc = DomainCache(2, self.conn, 'domain_metadata', batch_size=3)
        dc['d1'] = {'domain': 1}
        dc['d2'] = {'domain': 2}

        dc['d3'] = {'domain': [3, 2, 1]}
        dc['d4'] = {'domain': 4}

        dc.setdefault('d1', {})['domain'] += 1

        assert dc.setdefault('d1', {}) == {'domain': 2}

    def test_empty_key(self):
        dc = DomainCache(2, self.conn, 'domain_metadata')
        with self.assertRaises(KeyError):
            dc[''] = {'test':1}

    def test_deletion(self):
        dc = DomainCache(2, self.conn, 'domain_metadata')
        with self.assertRaises(KeyError):
            del dc['d1']

        dc['d1'] = {'domain': 1}
        dc['d2'] = {'domain': 2}
        dc['d3'] = {'domain': [3, 2, 1]}
        dc['d4'] = {'domain': 4}

        del dc['d1'] # second gen
        del dc['d3'] # first gen

        dc.flush()

        del dc['d4'] # hbase

    def test_contains(self):
        dc = DomainCache(2, self.conn, 'domain_metadata')
        dc['d1'] = {'domain': 1}
        dc['d2'] = {'domain': 2}
        dc['d3'] = {'domain': [3, 2, 1]}
        dc['d4'] = {'domain': 4}

        assert 'd1' in dc # second gen
        assert 'd3' in dc # first gen

        dc.flush()

        assert 'd4' in dc

    def test_pop(self):
        dc = DomainCache(2, self.conn, 'domain_metadata')
        dc['d1'] = {'domain': 1}
        dc['d2'] = {'domain': 2}
        dc['d3'] = {'domain': [3, 2, 1]}
        dc['d4'] = {'domain': 4}

        assert dc.pop('d1') == {'domain': 1}
        assert 'd1' not in dc

        assert dc.pop('d3') == {'domain': [3, 2, 1]}
        assert 'd3' not in dc

        dc.flush()

        assert dc.pop('d4') == {'domain': 4}
        assert 'd4' not in dc