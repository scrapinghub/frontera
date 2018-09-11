# -*- coding: utf-8 -*-
from __future__ import absolute_import
from frontera.contrib.backends.redis_backend import FIELD_DOMAIN_FINGERPRINT, FIELD_ERROR, FIELD_STATE
from frontera.contrib.backends.redis_backend import FIELD_STATUS_CODE, FIELD_URL
from frontera.contrib.backends.redis_backend import RedisBackend, RedisMetadata, RedisQueue, RedisState
from frontera.core.manager import WorkerFrontierManager
from frontera.settings import Settings
from redis import ConnectionPool, StrictRedis
from time import time
from unittest import main, TestCase

from logging import basicConfig, INFO

basicConfig(level=INFO)


class Request:
    def __init__(self, fingerprint, crawl_at, url, domain=None):
        self.meta = {
            b'crawl_at': crawl_at,
            b'fingerprint': fingerprint
        }
        if domain:
            self.meta[b'domain'] = {b'name': domain, b'fingerprint': "d_{}".format(fingerprint)}
        self.url = url
        self.method = 'https'
        self.headers = {}
        self.cookies = None
        self.status_code = 200


def get_pool():
    port = 6379
    host = 'localhost'
    return ConnectionPool(host=host, port=port, db=0)


class RedisQueueTest(TestCase):
    @staticmethod
    def setup_subject(partitions):
        settings = Settings(module='frontera.settings.default_settings')
        return RedisQueue(WorkerFrontierManager.from_settings(settings), get_pool(), partitions, True)

    def test_scheduling_past_1part_5(self):
        subject = self.setup_subject(1)
        batch = [
            ("1", 1, Request("1", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com'), True),
            ("2", 0.1, Request("2", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com'), True),
            ("3", 0.5, Request("3", int(time()) - 10, 'https://www.hellan.me/', domain='hellan.me'), True),
        ]
        subject.schedule(batch)
        self.assertEqual(3, subject.count())
        requests = subject.get_next_requests(5, 0, min_hosts=1, min_requests=1, max_requests_per_host=5)
        self.assertEqual(3, len(requests))
        urls = [request.url for request in requests]
        self.assertTrue('https://www.knuthellan.com/' in urls)
        self.assertTrue('https://www.hellan.me/' in urls)
        self.assertTrue('https://www.khellan.com/' in urls)
        self.assertEqual(0, subject.count())

    def test_scheduling_past_1part_1(self):
        subject = self.setup_subject(1)
        batch = [
            ("1", 1, Request("1", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com'), True),
            ("2", 0.1, Request("2", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com'), True),
            ("3", 0.5, Request("3", int(time()) - 10, 'https://www.hellan.me/', domain='hellan.me'), True),
        ]
        subject.schedule(batch)
        self.assertEqual(3, subject.count())
        requests = subject.get_next_requests(1, 0, min_hosts=1, min_requests=1, max_requests_per_host=5)
        self.assertEqual(1, len(requests))
        urls = [request.url for request in requests]
        self.assertTrue('https://www.knuthellan.com/' in urls)
        self.assertEqual(2, subject.count())

    def test_scheduling_past_1part_2(self):
        subject = self.setup_subject(1)
        batch = [
            ("1", 1, Request("1", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com'), True),
            ("2", 0.1, Request("2", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com'), True),
            ("3", 0.5, Request("3", int(time()) - 10, 'https://www.hellan.me/', domain='hellan.me'), True),
        ]
        subject.schedule(batch)
        self.assertEqual(3, subject.count())
        requests = subject.get_next_requests(2, 0, min_hosts=1, min_requests=1, max_requests_per_host=5)
        self.assertEqual(2, len(requests))
        urls = [request.url for request in requests]
        self.assertTrue('https://www.knuthellan.com/' in urls)
        self.assertTrue('https://www.hellan.me/' in urls)
        self.assertEqual(1, subject.count())

    def test_scheduling_past_2part_5(self):
        subject = self.setup_subject(2)
        batch = [
            ("1", 1, Request("1", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com'), True),
            ("2", 0.1, Request("2", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com'), True),
            ("3", 0.5, Request("3", int(time()) - 10, 'https://www.hellan.me/', domain='hellan.me'), True),
        ]
        subject.schedule(batch)
        self.assertEqual(3, subject.count())

        requests = subject.get_next_requests(5, partition_id=0, min_hosts=1, min_requests=1, max_requests_per_host=5)
        self.assertEqual(2, len(requests))
        urls = [request.url for request in requests]
        self.assertTrue('https://www.knuthellan.com/' in urls)
        self.assertTrue('https://www.khellan.com/' in urls)
        self.assertEqual(1, subject.count())

        requests = subject.get_next_requests(5, partition_id=1, min_hosts=1, min_requests=1, max_requests_per_host=5)
        self.assertEqual(1, len(requests))
        urls = [request.url for request in requests]
        self.assertTrue('https://www.hellan.me/' in urls)
        self.assertEqual(0, subject.count())

    def test_scheduling_past_2part_2(self):
        subject = self.setup_subject(2)
        batch = [
            ("1", 1, Request("1", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com'), True),
            ("2", 0.1, Request("2", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com'), True),
            ("3", 0.5, Request("3", int(time()) - 10, 'https://www.hellan.me/', domain='hellan.me'), True),
        ]
        subject.schedule(batch)
        self.assertEqual(3, subject.count())
        requests = subject.get_next_requests(2, partition_id=0, min_hosts=1, min_requests=1, max_requests_per_host=5)
        self.assertEqual(2, len(requests))
        urls = [request.url for request in requests]
        self.assertTrue('https://www.knuthellan.com/' in urls)
        self.assertTrue('https://www.khellan.com/' in urls)
        self.assertEqual(1, subject.count())

        requests = subject.get_next_requests(2, partition_id=1, min_hosts=1, min_requests=1, max_requests_per_host=5)
        self.assertEqual(1, len(requests))
        urls = [request.url for request in requests]
        self.assertTrue('https://www.hellan.me/' in urls)
        self.assertEqual(0, subject.count())

    def test_scheduling_past_2part_1(self):
        subject = self.setup_subject(2)
        batch = [
            ("1", 1, Request("1", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com'), True),
            ("2", 0.1, Request("2", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com'), True),
            ("3", 0.5, Request("3", int(time()) - 10, 'https://www.hellan.me/', domain='hellan.me'), True),
        ]
        subject.schedule(batch)
        self.assertEqual(3, subject.count())
        requests = subject.get_next_requests(1, partition_id=0, min_hosts=1, min_requests=1, max_requests_per_host=5)
        self.assertEqual(1, len(requests))
        urls = [request.url for request in requests]
        self.assertTrue('https://www.knuthellan.com/' in urls)

        requests = subject.get_next_requests(1, partition_id=1, min_hosts=1, min_requests=1, max_requests_per_host=5)
        self.assertEqual(1, len(requests))
        urls = [request.url for request in requests]
        self.assertTrue('https://www.hellan.me/' in urls)
        self.assertEqual(1, subject.count())

    def test_scheduling_past_2part_multiple(self):
        subject = self.setup_subject(2)
        batch = [
            ("1", 1, Request("1", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com'), True),
            ("2", 0.1, Request("2", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com'), True),
            ("3", 0.5, Request("3", int(time()) - 10, 'https://www.hellan.me/', domain='hellan.me'), True),
        ]
        subject.schedule(batch)
        self.assertEqual(3, subject.count())

        requests = subject.get_next_requests(1, partition_id=0, min_hosts=1, min_requests=1, max_requests_per_host=5)
        self.assertEqual(1, len(requests))
        urls = [request.url for request in requests]
        self.assertTrue('https://www.knuthellan.com/' in urls)
        self.assertEqual(2, subject.count())

        requests = subject.get_next_requests(1, partition_id=1, min_hosts=1, min_requests=1, max_requests_per_host=5)
        self.assertEqual(1, len(requests))
        urls = [request.url for request in requests]
        self.assertTrue('https://www.hellan.me/' in urls)
        self.assertEqual(1, subject.count())

        requests = subject.get_next_requests(1, partition_id=0, min_hosts=1, min_requests=1, max_requests_per_host=5)
        self.assertEqual(1, len(requests))
        urls = [request.url for request in requests]
        self.assertTrue('https://www.khellan.com/' in urls)
        self.assertEqual(0, subject.count())

        requests = subject.get_next_requests(1, partition_id=1, min_hosts=1, min_requests=1, max_requests_per_host=5)
        self.assertEqual(0, len(requests))

        requests = subject.get_next_requests(1, partition_id=0, min_hosts=1, min_requests=1, max_requests_per_host=5)
        self.assertEqual(0, len(requests))

    def test_scheduling_future(self):
        subject = self.setup_subject(1)
        batch = [
            ("1", 1, Request("1", int(time()) + 86400, 'https://www.knuthellan.com/', domain='knuthellan.com'), True),
            ("2", 0.1, Request("2", int(time()) + 86400, 'https://www.khellan.com/', domain='khellan.com'), True),
            ("3", 0.5, Request("3", int(time()) + 86400, 'https://www.hellan.me/', domain='hellan.me'), True),
        ]
        subject.schedule(batch)
        self.assertEqual(3, subject.count())

        requests = subject.get_next_requests(5, 0, min_hosts=1, min_requests=1, max_requests_per_host=5)
        self.assertEqual(0, len(requests))

    def test_scheduling_mix(self):
        subject = self.setup_subject(1)
        batch = [
            ("1", 1, Request("1", int(time()) + 86400, 'https://www.knuthellan.com/', domain='knuthellan.com'), True),
            ("2", 0.1, Request("2", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com'), True),
            ("3", 0.5, Request("3", int(time()) + 86400, 'https://www.hellan.me/', domain='hellan.me'), True),
        ]
        subject.schedule(batch)
        self.assertEqual(3, subject.count())

        requests = subject.get_next_requests(5, 0, min_hosts=1, min_requests=1, max_requests_per_host=5)
        self.assertEqual(1, len(requests))
        urls = [request.url for request in requests]
        self.assertTrue('https://www.khellan.com/' in urls)
        self.assertEqual(2, subject.count())

    def test_scheduling_conflict_high_score_high_timestamp(self):
        subject = self.setup_subject(1)
        batch = [
            ("1", 1, Request("1", int(time()) + 86400, 'https://www.knuthellan.com/', domain='knuthellan.com'), True),
            ("2", 0.1, Request("2", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com'), True),
            ("3", 0.5, Request("3", int(time()) + 86400, 'https://www.hellan.me/', domain='hellan.me'), True),
            ("4", 0.7, Request("3", int(time()) + 86400, 'https://www.hellan.me/', domain='hellan.me'), True),
            ("5", 0.8, Request("3", int(time()) + 86400, 'https://www.hellan.me/', domain='hellan.me'), True),
            ("6", 0.9, Request("3", int(time()) + 86400, 'https://www.hellan.me/', domain='hellan.me'), True),
        ]
        subject.schedule(batch)
        self.assertEqual(6, subject.count())

        requests = subject.get_next_requests(2, 0, min_hosts=1, min_requests=1, max_requests_per_host=5)
        self.assertEqual(1, len(requests))
        urls = [request.url for request in requests]
        self.assertTrue('https://www.khellan.com/' in urls)
        self.assertEqual(5, subject.count())

    def test_get_next_requests_max_requests(self):
        subject = self.setup_subject(2)
        batch = [
            ("1", 1, Request("1", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com'), True),
            ("2", 0.1, Request("2", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com'), True),
            ("3", 0.5, Request("3", int(time()) - 10, 'https://www.hellan.me/', domain='hellan.me'), True),
        ]
        subject.schedule(batch)
        self.assertEqual(3, subject.count())
        requests = subject.get_next_requests(1, partition_id=0, min_hosts=1, min_requests=1, max_requests_per_host=5)
        self.assertEqual(1, len(requests))
        urls = [request.url for request in requests]
        self.assertTrue('https://www.knuthellan.com/' in urls)
        self.assertEqual(2, subject.count())

    def test_get_next_requests_min_hosts(self):
        subject = self.setup_subject(2)
        batch = [
            ("1", 1, Request("1", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com'), True),
            ("2", 0.1, Request("2", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com'), True),
            ("3", 0.5, Request("3", int(time()) - 10, 'https://www.hellan.me/', domain='hellan.me'), True),
        ]
        subject.schedule(batch)
        self.assertEqual(3, subject.count())
        requests = subject.get_next_requests(1, partition_id=0, min_hosts=2, min_requests=1, max_requests_per_host=5)
        self.assertEqual(2, len(requests))
        urls = [request.url for request in requests]
        self.assertTrue('https://www.knuthellan.com/' in urls)
        self.assertTrue('https://www.khellan.com/' in urls)
        self.assertEqual(1, subject.count())

    def test_get_next_requests_min_hosts_high_number(self):
        subject = self.setup_subject(2)
        batch = [
            ("1", 1, Request("1", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com'), True),
            ("2", 0.1, Request("2", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com'), True),
            ("3", 0.5, Request("3", int(time()) - 10, 'https://www.hellan.me/', domain='hellan.me'), True),
        ]
        subject.schedule(batch)
        self.assertEqual(3, subject.count())
        requests = subject.get_next_requests(1, partition_id=0, min_hosts=5, min_requests=1, max_requests_per_host=5)
        self.assertEqual(2, len(requests))
        urls = [request.url for request in requests]
        self.assertTrue('https://www.knuthellan.com/' in urls)
        self.assertTrue('https://www.khellan.com/' in urls)
        self.assertEqual(1, subject.count())

    def test_get_next_requests_max_requests_2(self):
        subject = self.setup_subject(2)
        batch = [
            ("1", 1, Request("1", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com'), True),
            ("1", 0.99, Request("1", int(time()) - 10, 'https://www.knuthellan.com/a', domain='knuthellan.com'), True),
            ("1", 0.98, Request("1", int(time()) - 10, 'https://www.knuthellan.com/c', domain='knuthellan.com'), True),
            ("2", 0.1, Request("2", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com'), True),
            ("3", 0.5, Request("3", int(time()) - 10, 'https://www.hellan.me/', domain='hellan.me'), True),
        ]
        subject.schedule(batch)
        self.assertEqual(5, subject.count())
        requests = subject.get_next_requests(5, partition_id=0, min_hosts=1, min_requests=1, max_requests_per_host=2)
        self.assertGreaterEqual(len(requests), 2)
        urls = [request.url for request in requests]
        self.assertTrue('https://www.knuthellan.com/' in urls)
        self.assertTrue('https://www.knuthellan.com/a' in urls)
        self.assertFalse('https://www.knuthellan.com/c' in urls)

    def test_get_next_requests_few_items_few_hosts(self):
        subject = self.setup_subject(2)
        batch = [
            ("1", 1, Request("1", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com'), True)
        ]
        subject.schedule(batch)
        self.assertEqual(1, subject.count())
        requests = subject.get_next_requests(1, partition_id=0, min_hosts=2, min_requests=1, max_requests_per_host=5)
        self.assertEqual(1, len(requests))
        urls = [request.url for request in requests]
        self.assertTrue('https://www.knuthellan.com/' in urls)
        self.assertEqual(0, subject.count())



class RedisStateTest(TestCase):
    def test_update_cache(self):
        subject = RedisState(get_pool(), 10)
        r1 = Request("1", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com')
        r1.meta[b'state'] = b'a'
        r2 = Request("2", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com')
        r2.meta[b'state'] = b'b'
        r3 = Request("3", int(time()) - 10, 'https://www.hellan.me/', domain='hellan.me')
        r3.meta[b'state'] = b'c'
        batch = [r1, r2, r3]
        subject.update_cache(batch)
        self.assertEqual(3, len(subject._cache))
        self.assertEqual(b'a', subject._cache["1"])
        self.assertEqual(b'b', subject._cache["2"])
        self.assertEqual(b'c', subject._cache["3"])

    def test_set_states(self):
        subject = RedisState(get_pool(), 10)
        r1 = Request("1", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com')
        r1.meta[b'state'] = b'a'
        r2 = Request("2", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com')
        r2.meta[b'state'] = b'b'
        r3 = Request("3", int(time()) - 10, 'https://www.hellan.me/', domain='hellan.me')
        r3.meta[b'state'] = b'c'
        batch = [r1, r2, r3]
        subject.update_cache(batch)
        r4 = Request("1", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com')
        r5 = Request("2", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com')
        r6 = Request("3", int(time()) - 10, 'https://www.hellan.me/', domain='hellan.me')
        batch2 = [r4, r5, r6]
        subject.set_states(batch2)
        self.assertEqual(b'a', r4.meta[b'state'])
        self.assertEqual(b'b', r5.meta[b'state'])
        self.assertEqual(b'c', r6.meta[b'state'])

    def test_flush_no_force(self):
        pool = get_pool()
        subject = RedisState(pool, 10)
        r1 = Request("1", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com')
        r1.meta[b'state'] = b'a'
        r2 = Request("2", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com')
        r2.meta[b'state'] = b'b'
        r3 = Request("3", int(time()) - 10, 'https://www.hellan.me/', domain='hellan.me')
        r3.meta[b'state'] = b'c'
        batch = [r1, r2, r3]
        subject.update_cache(batch)
        subject.flush(False)
        self.assertEqual(3, len(subject._cache))
        connection = StrictRedis(connection_pool=pool)
        self.assertEqual({FIELD_STATE: b'a'}, connection.hgetall("1"))
        self.assertEqual({FIELD_STATE: b'b'}, connection.hgetall("2"))
        self.assertEqual({FIELD_STATE: b'c'}, connection.hgetall("3"))

    def test_flush_force(self):
        pool = get_pool()
        subject = RedisState(pool, 10)
        r1 = Request("4", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com')
        r1.meta[b'state'] = b'd'
        r2 = Request("5", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com')
        r2.meta[b'state'] = b'e'
        r3 = Request("6", int(time()) - 10, 'https://www.hellan.me/', domain='hellan.me')
        r3.meta[b'state'] = b'f'
        batch = [r1, r2, r3]
        subject.update_cache(batch)
        subject.flush(True)
        self.assertEqual(0, len(subject._cache))
        connection = StrictRedis(connection_pool=pool)
        self.assertEqual({FIELD_STATE: b'd'}, connection.hgetall("4"))
        self.assertEqual({FIELD_STATE: b'e'}, connection.hgetall("5"))
        self.assertEqual({FIELD_STATE: b'f'}, connection.hgetall("6"))

    def test_flush_cache_overflow(self):
        pool = get_pool()
        subject = RedisState(pool, 1)
        r1 = Request("4", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com')
        r1.meta[b'state'] = b'd'
        r2 = Request("5", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com')
        r2.meta[b'state'] = b'e'
        r3 = Request("6", int(time()) - 10, 'https://www.hellan.me/', domain='hellan.me')
        r3.meta[b'state'] = b'f'
        batch = [r1, r2, r3]
        subject.update_cache(batch)
        subject.flush(False)
        self.assertEqual(0, len(subject._cache))
        connection = StrictRedis(connection_pool=pool)
        self.assertEqual({FIELD_STATE: b'd'}, connection.hgetall("4"))
        self.assertEqual({FIELD_STATE: b'e'}, connection.hgetall("5"))
        self.assertEqual({FIELD_STATE: b'f'}, connection.hgetall("6"))

    def test_fetch(self):
        subject = RedisState(get_pool(), 1)
        r1 = Request("7", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com')
        r1.meta[b'state'] = b'g'
        r2 = Request("8", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com')
        r2.meta[b'state'] = b'h'
        batch = [r1, r2]
        subject.update_cache(batch)
        subject.flush(True)
        r3 = Request("9", int(time()) - 10, 'https://www.hellan.me/', domain='hellan.me')
        r3.meta[b'state'] = b'i'
        subject.update_cache(r3)
        self.assertEqual(1, len(subject._cache))
        to_fetch = ["7", "9"]
        subject.fetch(to_fetch)
        self.assertEqual(2, len(subject._cache))
        self.assertEqual(b'g', subject._cache["7"])
        self.assertEqual(b'i', subject._cache["9"])


class RedisMetadataTest(TestCase):
    def test_add_seeds(self):
        pool = get_pool()
        subject = RedisMetadata(pool, True)
        r1 = Request("md1", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com')
        r2 = Request("md2", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com')
        r3 = Request("md3", int(time()) - 10, 'https://www.hellan.me/', domain='hellan.me')
        seeds = [r1, r2, r3]
        subject.add_seeds(seeds)
        connection = StrictRedis(connection_pool=pool)
        self.assertEqual(b'https://www.knuthellan.com/', connection.hmget('md1', FIELD_URL)[0])
        self.assertEqual(b'd_md1', connection.hmget('md1', FIELD_DOMAIN_FINGERPRINT)[0])
        self.assertEqual(b'https://www.khellan.com/', connection.hmget("md2", FIELD_URL)[0])
        self.assertEqual(b'd_md2', connection.hmget('md2', FIELD_DOMAIN_FINGERPRINT)[0])
        self.assertEqual(b'https://www.hellan.me/', connection.hmget("md3", FIELD_URL)[0])
        self.assertEqual(b'd_md3', connection.hmget('md3', FIELD_DOMAIN_FINGERPRINT)[0])

    def test_request_error(self):
        pool = get_pool()
        subject = RedisMetadata(pool, True)
        r1 = Request("md1", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com')
        subject.request_error(r1, 404)
        connection = StrictRedis(connection_pool=pool)
        self.assertEqual(b'https://www.knuthellan.com/', connection.hmget('md1', FIELD_URL)[0])
        self.assertEqual(b'd_md1', connection.hmget('md1', FIELD_DOMAIN_FINGERPRINT)[0])
        self.assertEqual(b'404', connection.hmget('md1', FIELD_ERROR)[0])

    def test_page_crawled(self):
        pool = get_pool()
        subject = RedisMetadata(pool, True)
        r1 = Request("md1", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com')
        subject.page_crawled(r1)
        connection = StrictRedis(connection_pool=pool)
        self.assertEqual(b'200', connection.hmget('md1', FIELD_STATUS_CODE)[0])

    def test_links_extracted(self):
        pool = get_pool()
        subject = RedisMetadata(pool, True)
        l1 = Request("l1", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com')
        l2 = Request("l2", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com')
        l3 = Request("l3", int(time()) - 10, 'https://www.hellan.me/', domain='hellan.me')
        links = [l1, l2, l3]
        subject.links_extracted(None, links)
        connection = StrictRedis(connection_pool=pool)
        self.assertEqual(b'https://www.knuthellan.com/', connection.hmget('l1', FIELD_URL)[0])
        self.assertEqual(b'd_l1', connection.hmget('l1', FIELD_DOMAIN_FINGERPRINT)[0])
        self.assertEqual(b'https://www.khellan.com/', connection.hmget("l2", FIELD_URL)[0])
        self.assertEqual(b'd_l2', connection.hmget('l2', FIELD_DOMAIN_FINGERPRINT)[0])
        self.assertEqual(b'https://www.hellan.me/', connection.hmget("l3", FIELD_URL)[0])
        self.assertEqual(b'd_l3', connection.hmget('l3', FIELD_DOMAIN_FINGERPRINT)[0])


class RedisBackendTest(TestCase):
    @staticmethod
    def setup_subject(partitions):
        settings = Settings(module='frontera.settings.default_settings')
        settings.set('SPIDER_FEED_PARTITIONS', partitions)
        settings.set('REDIS_DROP_ALL_TABLES', True)
        return RedisBackend.db_worker(WorkerFrontierManager.from_settings(settings, db_worker=True))

    def test_get_next_request(self):
        subject = self.setup_subject(2)
        requests = subject.get_next_requests(max_next_requests=10, partitions=['0', '1'])
        self.assertEqual(0, len(requests))

    def test_get_next_request_has_requests(self):
        subject = self.setup_subject(2)
        batch = [
            ("1", 1, Request("1", int(time()) - 10, 'https://www.knuthellan.com/', domain='knuthellan.com'), True),
            ("2", 0.1, Request("2", int(time()) - 10, 'https://www.khellan.com/', domain='khellan.com'), True),
            ("3", 0.5, Request("3", int(time()) - 10, 'https://www.hellan.me/', domain='hellan.me'), True),
        ]
        subject.queue.schedule(batch)
        requests = subject.get_next_requests(max_next_requests=10, partitions=['0', '1'])
        self.assertEqual(3, len(requests))

    def test_close_manager(self):
        settings = Settings(module='frontera.settings.default_settings')
        settings.set('BACKEND', 'frontera.contrib.backends.redis_backend.RedisBackend')
        manager = WorkerFrontierManager.from_settings(settings, strategy_worker=True)
        self.assertEqual(RedisBackend, manager.backend.__class__)
        manager.close()


if __name__ == '__main__':
    main()
