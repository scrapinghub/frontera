# -*- coding: utf-8 -*-
from __future__ import absolute_import
from tests.backends import BackendSequenceTest, TEST_SITES
from frontera.utils.tester import DownloaderSimulator, BaseDownloaderSimulator
from six.moves.urllib.parse import urlparse


class DFSOverusedBackendTest(BackendSequenceTest):

    EXPECTED_SEQUENCES = {
        "SEQUENCE_01_A": [
            'https://www.a.com', 'http://b.com', 'http://www.a.com/2', 'http://www.a.com/2/1', 'http://www.a.com/3',
            'http://www.a.com/2/1/3', 'http://www.a.com/2/4/1', 'http://www.a.net', 'http://b.com/2',
            'http://test.cloud.c.com', 'http://cloud.c.com', 'http://test.cloud.c.com/2',
            'http://b.com/entries?page=2', 'http://www.a.com/2/4/2'
        ],
        "SEQUENCE_02_A": [
            'https://www.a.com', 'http://b.com', 'http://www.a.com/2', 'http://www.a.com/2/1', 'http://www.a.com/3',
            'http://www.a.com/2/1/3', 'http://www.a.com/2/4/1', 'http://www.a.com/2/4/2', 'http://www.a.net',
            'http://b.com/2', 'http://test.cloud.c.com', 'http://cloud.c.com', 'http://test.cloud.c.com/2',
            'http://b.com/entries?page=2'
        ]
    }

    def get_settings(self):
        settings = super(DFSOverusedBackendTest, self).get_settings()
        settings.TEST_MODE = True
        settings.LOGGING_MANAGER_ENABLED = False
        settings.LOGGING_BACKEND_ENABLED = False
        settings.LOGGING_DEBUGGING_ENABLED = False
        settings.STRATEGY = 'tests.backends.DFSCrawlingStrategy'
        return settings

    def test_sequence1(self):
        sequence = self.get_sequence(TEST_SITES['SITE_09'], max_next_requests=5,
                                     downloader_simulator=DownloaderSimulator(rate=1))
        assert len(sequence) == 7
        all_domains = set()
        for requests, iteration, dl_info in sequence:
            overused_keys = set(dl_info['overused_keys'])
            for r in requests:
                url = urlparse(r.url)
                all_domains.add(url.hostname)
                if not overused_keys:
                    continue
                assert url.hostname not in overused_keys
            assert overused_keys.issubset(all_domains)
