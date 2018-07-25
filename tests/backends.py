from __future__ import absolute_import
import pytest

from frontera.core.components import States
from frontera.core.manager import LocalFrontierManager
from frontera.strategy import BaseCrawlingStrategy
from frontera import Settings, FrontierTester
from frontera.utils import graphs
from frontera.utils.tester import BaseDownloaderSimulator


class BasicCrawlingStrategy(BaseCrawlingStrategy):
    def __init__(self, manager, args, scheduled_stream, states_context):
        super(BasicCrawlingStrategy, self).__init__(manager, args, scheduled_stream, states_context)
        self._id = 0

    def read_seeds(self, stream):
        for url in stream:
            url = url.strip()
            r = self._create_request(url)
            self.schedule(r)

    def _create_request(self, url):
        r = self.create_request(url=url,
                                headers={
                                    b'X-Important-Header': b'Frontera'
                                },
                                method=b'POST',
                                cookies={b'currency': b'USD'},
                                meta={b'this_param': b'should be passed over',
                                      b'id': self._id})
        self._id += 1
        return r

    def filter_extracted_links(self, request, links):
        return links

    def links_extracted(self, request, links):
        for link in links:
            if link.meta[b'state'] == States.NOT_CRAWLED:
                self.schedule(self._create_request(link.url))
                link.meta[b'state'] = States.QUEUED

    def page_crawled(self, response):
        response.meta[b'state'] = States.CRAWLED

    def request_error(self, request, error):
        request.meta[b'state'] = States.ERROR


class DFSCrawlingStrategy(BasicCrawlingStrategy):
    def read_seeds(self, stream):
        for url in stream:
            url = url.strip()
            r = self._create_request(url)
            r.meta[b'depth'] = 0
            self.schedule(r, self._get_score(r.meta[b'depth']))

    def links_extracted(self, request, links):
        for link in links:
            if link.meta[b'state'] == States.NOT_CRAWLED:
                r = self._create_request(link.url)
                r.meta[b'depth'] = request.meta[b'depth'] + 1
                self.schedule(r, self._get_score(r.meta[b'depth']))
                link.meta[b'state'] = States.QUEUED

    def _get_score(self, depth):
        return 1.0 / (depth + 1.0)


class BFSCrawlingStrategy(DFSCrawlingStrategy):
    def _get_score(self, depth):
        return float(depth) / 10.0


class BackendTest(object):
    """
    A simple pytest base class with helper methods for
    :class:`Backend <frontera.core.components.Backend>` testing.
    """

    backend_class = None

    def setup_method(self, method):
        if not self.backend_class:
            pytest.fail('missing backend_class!')
        self.setup_backend(method)

    def teardown_method(self, method):
        self.teardown_backend(method)

    def setup_backend(self, method):
        """
        Setup method called before each test method call
        """
        pass

    def teardown_backend(self, method):
        """
        Teardown method called after each test method call
        """
        pass

    def get_frontier(self):
        """
        Returns frontierManager object
        """
        return LocalFrontierManager.from_settings(self.get_settings())

    def get_settings(self):
        """
        Returns backend settings
        """
        return Settings(attributes={
            'BACKEND': self.backend_class,
            'STRATEGY': 'tests.backends.BasicCrawlingStrategy'
        })


TEST_SITES = {
    "SITE_01": graphs.data.SITE_LIST_01,
    "SITE_02": graphs.data.SITE_LIST_02,
    "SITE_03": graphs.data.SITE_LIST_03,
    "SITE_09": graphs.data.SITE_LIST_09,
}


class BackendSequenceTest(BackendTest):
    """
    A pytest base class for testing
    :class:`Backend <frontera.core.components.Backend>` crawling sequences.
    """
    def get_settings(self):
        settings = super(BackendSequenceTest, self).get_settings()
        settings.TEST_MODE = True
        settings.LOGGING_MANAGER_ENABLED = False
        settings.LOGGING_BACKEND_ENABLED = False
        settings.LOGGING_DEBUGGING_ENABLED = False
        return settings

    def get_sequence(self, site_list, max_next_requests, downloader_simulator=BaseDownloaderSimulator(),
                     frontier_tester=FrontierTester):
        """
        Returns an Frontera iteration sequence from a site list

        :param list site_list: A list of sites to use as frontier seeds.
        :param int max_next_requests: Max next requests for the frontier.
        """

        # Graph
        graph_manager = graphs.Manager()
        graph_manager.add_site_list(site_list)

        # Tester
        tester = frontier_tester(frontier=self.get_frontier(),
                                 graph_manager=graph_manager,
                                 max_next_requests=max_next_requests,
                                 downloader_simulator=downloader_simulator)
        tester.run()
        return tester.sequence

    def get_url_sequence(self, site_list, max_next_requests, downloader_simulator=BaseDownloaderSimulator(),
                         frontier_tester=FrontierTester):
        """
        Returns a crawling sequence from a site list

        :param list site_list: A list of sites to use as frontier seeds.
        :param int max_next_requests: Max next requests for the frontier.
        """
        sequence = []
        for requests, iteration, dl_info in self.get_sequence(site_list, max_next_requests, downloader_simulator,
                                                              frontier_tester):
            sequence.extend([r.url for r in requests])
        return sequence

    def assert_sequence(self, site_list, expected_sequence, max_next_requests):
        """
        Asserts that crawling sequence is the one expected

        :param list site_list: A list of sites to use as frontier seeds.
        :param int max_next_requests: Max next requests for the frontier.
        """

        # Get sequence
        sequence = self.get_url_sequence(site_list, max_next_requests)
        #print ([str(n) for n in sequence])

        # Assert sequence equals expected
        assert len(sequence) == len(expected_sequence)
        assert sequence == expected_sequence


class FIFOBackendTest(BackendSequenceTest):
    EXPECTED_SEQUENCES = {
        "SEQUENCE_01_A": [
             'http://aaa.com/1',
             'http://aaa.com/11', 'http://aaa.com/12',
             'http://aaa.com/111', 'http://aaa.com/112', 'http://aaa.com/121', 'http://aaa.com/122',
             'http://aaa.com/1111', 'http://aaa.com/1112', 'http://aaa.com/1121', 'http://aaa.com/1122', 'http://aaa.com/1211', 'http://aaa.com/1212', 'http://aaa.com/1221', 'http://aaa.com/1222'
        ],
        "SEQUENCE_02_A": [
            'http://aaa.com/1', 'http://bbb.com/1',
            'http://aaa.com/11', 'http://aaa.com/12', 'http://bbb.com/11', 'http://bbb.com/12',
            'http://aaa.com/111', 'http://aaa.com/112', 'http://aaa.com/121', 'http://aaa.com/122', 'http://bbb.com/111', 'http://bbb.com/112', 'http://bbb.com/121', 'http://bbb.com/122',
            'http://aaa.com/1111', 'http://aaa.com/1112', 'http://aaa.com/1121', 'http://aaa.com/1122', 'http://aaa.com/1211', 'http://aaa.com/1212', 'http://aaa.com/1221', 'http://aaa.com/1222', 'http://bbb.com/1111', 'http://bbb.com/1112', 'http://bbb.com/1121', 'http://bbb.com/1122', 'http://bbb.com/1211', 'http://bbb.com/1212', 'http://bbb.com/1221', 'http://bbb.com/1222'
        ]
        ,
        "SEQUENCE_03_A": [
            'http://ccc.com/1',
            'http://ccc.com/11', 'http://ccc.com/12',
            'http://ccc.com/111', 'http://ccc.com/112', 'http://ccc.com/121', 'http://ccc.com/122',
            'http://ccc.com/1111', 'http://ccc.com/1112', 'http://ccc.com/1121', 'http://ccc.com/1122', 'http://ccc.com/1211', 'http://ccc.com/1212', 'http://ccc.com/1221', 'http://ccc.com/1222',
            'http://ccc.com/11111', 'http://ccc.com/11112', 'http://ccc.com/11121', 'http://ccc.com/11122', 'http://ccc.com/11211', 'http://ccc.com/11212', 'http://ccc.com/11221', 'http://ccc.com/11222', 'http://ccc.com/12111', 'http://ccc.com/12112', 'http://ccc.com/12121', 'http://ccc.com/12122', 'http://ccc.com/12211', 'http://ccc.com/12212', 'http://ccc.com/12221', 'http://ccc.com/12222'
        ],
    }


    @pytest.mark.parametrize(
        ('site_list', 'max_next_requests', 'expected_sequence'), [

            ('SITE_01', 1, 'SEQUENCE_01_A'),
            ('SITE_01', 2, 'SEQUENCE_01_A'),
            ('SITE_01', 5, 'SEQUENCE_01_A'),
            ('SITE_01', 10, 'SEQUENCE_01_A'),
            ('SITE_01', 100, 'SEQUENCE_01_A'),

            ('SITE_02', 1, 'SEQUENCE_02_A'),
            ('SITE_02', 2, 'SEQUENCE_02_A'),
            ('SITE_02', 5, 'SEQUENCE_02_A'),
            ('SITE_02', 10, 'SEQUENCE_02_A'),
            ('SITE_02', 100, 'SEQUENCE_02_A'),

            ('SITE_03', 1, 'SEQUENCE_03_A'),
            ('SITE_03', 2, 'SEQUENCE_03_A'),
            ('SITE_03', 5, 'SEQUENCE_03_A'),
            ('SITE_03', 10, 'SEQUENCE_03_A'),
            ('SITE_03', 100, 'SEQUENCE_03_A'),
        ]
    )
    def test_sequence(self, site_list, max_next_requests, expected_sequence):
        self.assert_sequence(
            site_list=TEST_SITES[site_list],
            expected_sequence=self.EXPECTED_SEQUENCES[expected_sequence],
            max_next_requests=max_next_requests,
        )


class LIFOBackendTest(BackendSequenceTest):

    EXPECTED_SEQUENCES = {
        "SEQUENCE_01_A": [
            'http://aaa.com/1',
            'http://aaa.com/12',
            'http://aaa.com/122', 'http://aaa.com/1222', 'http://aaa.com/1221',
            'http://aaa.com/121', 'http://aaa.com/1212', 'http://aaa.com/1211',
            'http://aaa.com/11',
            'http://aaa.com/112', 'http://aaa.com/1122', 'http://aaa.com/1121',
            'http://aaa.com/111', 'http://aaa.com/1112', 'http://aaa.com/1111'
        ],
        "SEQUENCE_01_B": [
            'http://aaa.com/1',
            'http://aaa.com/12', 'http://aaa.com/11',
            'http://aaa.com/112', 'http://aaa.com/111',
            'http://aaa.com/1112', 'http://aaa.com/1111', 'http://aaa.com/1122', 'http://aaa.com/1121',
            'http://aaa.com/122', 'http://aaa.com/121',
            'http://aaa.com/1212', 'http://aaa.com/1211', 'http://aaa.com/1222', 'http://aaa.com/1221'],
        "SEQUENCE_01_C": [
            'http://aaa.com/1',
            'http://aaa.com/12', 'http://aaa.com/11',
            'http://aaa.com/112', 'http://aaa.com/111', 'http://aaa.com/122', 'http://aaa.com/121',
            'http://aaa.com/1212', 'http://aaa.com/1211', 'http://aaa.com/1222', 'http://aaa.com/1221', 'http://aaa.com/1112', 'http://aaa.com/1111', 'http://aaa.com/1122', 'http://aaa.com/1121'
        ],
        "SEQUENCE_02_A": [
            'http://bbb.com/1',
            'http://bbb.com/12', 'http://bbb.com/122', 'http://bbb.com/1222', 'http://bbb.com/1221', 'http://bbb.com/121', 'http://bbb.com/1212', 'http://bbb.com/1211',
            'http://bbb.com/11', 'http://bbb.com/112', 'http://bbb.com/1122', 'http://bbb.com/1121', 'http://bbb.com/111', 'http://bbb.com/1112', 'http://bbb.com/1111',
            'http://aaa.com/1',
            'http://aaa.com/12', 'http://aaa.com/122', 'http://aaa.com/1222', 'http://aaa.com/1221', 'http://aaa.com/121', 'http://aaa.com/1212', 'http://aaa.com/1211',
            'http://aaa.com/11', 'http://aaa.com/112', 'http://aaa.com/1122', 'http://aaa.com/1121', 'http://aaa.com/111', 'http://aaa.com/1112', 'http://aaa.com/1111'
        ],
        "SEQUENCE_02_B": [
            'http://bbb.com/1', 'http://aaa.com/1',
            'http://aaa.com/12', 'http://aaa.com/11',
            'http://aaa.com/112', 'http://aaa.com/111',
            'http://aaa.com/1112', 'http://aaa.com/1111', 'http://aaa.com/1122', 'http://aaa.com/1121',
            'http://aaa.com/122', 'http://aaa.com/121',
            'http://aaa.com/1212', 'http://aaa.com/1211', 'http://aaa.com/1222', 'http://aaa.com/1221',
            'http://bbb.com/12', 'http://bbb.com/11',
            'http://bbb.com/112', 'http://bbb.com/111',
            'http://bbb.com/1112', 'http://bbb.com/1111', 'http://bbb.com/1122', 'http://bbb.com/1121',
            'http://bbb.com/122', 'http://bbb.com/121',
            'http://bbb.com/1212', 'http://bbb.com/1211', 'http://bbb.com/1222', 'http://bbb.com/1221'
        ],
        "SEQUENCE_02_C": [
            'http://bbb.com/1', 'http://aaa.com/1',
            'http://aaa.com/12', 'http://aaa.com/11', 'http://bbb.com/12', 'http://bbb.com/11', 'http://bbb.com/112', 'http://bbb.com/111', 'http://bbb.com/122', 'http://bbb.com/121', 'http://aaa.com/112',
            'http://aaa.com/1122', 'http://aaa.com/1121', 'http://bbb.com/1212', 'http://bbb.com/1211', 'http://bbb.com/1222', 'http://bbb.com/1221', 'http://bbb.com/1112', 'http://bbb.com/1111', 'http://bbb.com/1122', 'http://bbb.com/1121',
            'http://aaa.com/111', 'http://aaa.com/122', 'http://aaa.com/121',
            'http://aaa.com/1212', 'http://aaa.com/1211', 'http://aaa.com/1222', 'http://aaa.com/1221', 'http://aaa.com/1112', 'http://aaa.com/1111'
        ],
        "SEQUENCE_02_D": [
            'http://bbb.com/1', 'http://aaa.com/1',
            'http://aaa.com/12', 'http://aaa.com/11', 'http://bbb.com/12', 'http://bbb.com/11', 'http://bbb.com/112', 'http://bbb.com/111', 'http://bbb.com/122', 'http://bbb.com/121', 'http://aaa.com/112', 'http://aaa.com/111', 'http://aaa.com/122', 'http://aaa.com/121',
            'http://aaa.com/1212', 'http://aaa.com/1211', 'http://aaa.com/1222', 'http://aaa.com/1221', 'http://aaa.com/1112', 'http://aaa.com/1111', 'http://aaa.com/1122', 'http://aaa.com/1121',
            'http://bbb.com/1212', 'http://bbb.com/1211', 'http://bbb.com/1222', 'http://bbb.com/1221', 'http://bbb.com/1112', 'http://bbb.com/1111', 'http://bbb.com/1122', 'http://bbb.com/1121'
        ],

        "SEQUENCE_03_A": [
            'http://ccc.com/1', 'http://ccc.com/12', 'http://ccc.com/122', 'http://ccc.com/1222', 'http://ccc.com/12222', 'http://ccc.com/12221', 'http://ccc.com/1221', 'http://ccc.com/12212', 'http://ccc.com/12211',
            'http://ccc.com/121', 'http://ccc.com/1212', 'http://ccc.com/12122', 'http://ccc.com/12121', 'http://ccc.com/1211', 'http://ccc.com/12112', 'http://ccc.com/12111',
            'http://ccc.com/11', 'http://ccc.com/112', 'http://ccc.com/1122', 'http://ccc.com/11222', 'http://ccc.com/11221', 'http://ccc.com/1121', 'http://ccc.com/11212', 'http://ccc.com/11211',
            'http://ccc.com/111', 'http://ccc.com/1112', 'http://ccc.com/11122', 'http://ccc.com/11121', 'http://ccc.com/1111', 'http://ccc.com/11112', 'http://ccc.com/11111'
        ],
        "SEQUENCE_03_B": [
            'http://ccc.com/1',
            'http://ccc.com/12', 'http://ccc.com/11',
            'http://ccc.com/112', 'http://ccc.com/111',
            'http://ccc.com/1112', 'http://ccc.com/1111', 'http://ccc.com/11112', 'http://ccc.com/11111', 'http://ccc.com/11122', 'http://ccc.com/11121',
            'http://ccc.com/1122', 'http://ccc.com/1121', 'http://ccc.com/11212', 'http://ccc.com/11211', 'http://ccc.com/11222', 'http://ccc.com/11221',
            'http://ccc.com/122', 'http://ccc.com/121',
            'http://ccc.com/1212', 'http://ccc.com/1211', 'http://ccc.com/12112', 'http://ccc.com/12111', 'http://ccc.com/12122', 'http://ccc.com/12121',
            'http://ccc.com/1222', 'http://ccc.com/1221', 'http://ccc.com/12212', 'http://ccc.com/12211', 'http://ccc.com/12222', 'http://ccc.com/12221'
        ],
        "SEQUENCE_03_C": [
            'http://ccc.com/1',
            'http://ccc.com/12', 'http://ccc.com/11',
            'http://ccc.com/112', 'http://ccc.com/111', 'http://ccc.com/122', 'http://ccc.com/121',
            'http://ccc.com/1212', 'http://ccc.com/1211', 'http://ccc.com/1222', 'http://ccc.com/1221', 'http://ccc.com/1112',
            'http://ccc.com/11122', 'http://ccc.com/11121', 'http://ccc.com/12212', 'http://ccc.com/12211',
            'http://ccc.com/12222', 'http://ccc.com/12221', 'http://ccc.com/12112', 'http://ccc.com/12111',
            'http://ccc.com/12122', 'http://ccc.com/12121',
            'http://ccc.com/1111', 'http://ccc.com/1122', 'http://ccc.com/1121', 'http://ccc.com/11212',
            'http://ccc.com/11211', 'http://ccc.com/11222', 'http://ccc.com/11221', 'http://ccc.com/11112', 'http://ccc.com/11111'
        ],
        "SEQUENCE_03_D": [
            'http://ccc.com/1',
            'http://ccc.com/12', 'http://ccc.com/11',
            'http://ccc.com/112', 'http://ccc.com/111', 'http://ccc.com/122', 'http://ccc.com/121',
            'http://ccc.com/1212', 'http://ccc.com/1211', 'http://ccc.com/1222', 'http://ccc.com/1221',
            'http://ccc.com/1112', 'http://ccc.com/1111', 'http://ccc.com/1122', 'http://ccc.com/1121',
            'http://ccc.com/11212', 'http://ccc.com/11211', 'http://ccc.com/11222', 'http://ccc.com/11221', 'http://ccc.com/11112', 'http://ccc.com/11111', 'http://ccc.com/11122', 'http://ccc.com/11121',
            'http://ccc.com/12212', 'http://ccc.com/12211', 'http://ccc.com/12222', 'http://ccc.com/12221', 'http://ccc.com/12112', 'http://ccc.com/12111', 'http://ccc.com/12122', 'http://ccc.com/12121'
        ],
    }

    @pytest.mark.parametrize(
        ('site_list', 'max_next_requests', 'expected_sequence'), [

            ('SITE_01', 1, 'SEQUENCE_01_A'),
            ('SITE_01', 2, 'SEQUENCE_01_B'),
            ('SITE_01', 5, 'SEQUENCE_01_C'),
            ('SITE_01', 10, 'SEQUENCE_01_C'),
            ('SITE_01', 100, 'SEQUENCE_01_C'),

            ('SITE_02', 1, 'SEQUENCE_02_A'),
            ('SITE_02', 2, 'SEQUENCE_02_B'),
            ('SITE_02', 5, 'SEQUENCE_02_C'),
            ('SITE_02', 10, 'SEQUENCE_02_D'),
            ('SITE_02', 100, 'SEQUENCE_02_D'),

            ('SITE_03', 1, 'SEQUENCE_03_A'),
            ('SITE_03', 2, 'SEQUENCE_03_B'),
            ('SITE_03', 5, 'SEQUENCE_03_C'),
            ('SITE_03', 10, 'SEQUENCE_03_D'),
            ('SITE_03', 100, 'SEQUENCE_03_D'),
        ]
    )
    def test_sequence(self, site_list, max_next_requests, expected_sequence):
        self.assert_sequence(
            site_list=TEST_SITES[site_list],
            expected_sequence=self.EXPECTED_SEQUENCES[expected_sequence],
            max_next_requests=max_next_requests,
        )


class DFSBackendTest(BackendSequenceTest):

    EXPECTED_SEQUENCES = {
        "SEQUENCE_01_A": [
            'http://aaa.com/1',
            'http://aaa.com/11', 'http://aaa.com/111', 'http://aaa.com/1111', 'http://aaa.com/1112', 'http://aaa.com/112', 'http://aaa.com/1121', 'http://aaa.com/1122',
            'http://aaa.com/12', 'http://aaa.com/121', 'http://aaa.com/1211', 'http://aaa.com/1212', 'http://aaa.com/122', 'http://aaa.com/1221', 'http://aaa.com/1222'
        ],
        "SEQUENCE_01_B": [
            'http://aaa.com/1',
            'http://aaa.com/11', 'http://aaa.com/12',
            'http://aaa.com/111', 'http://aaa.com/112',
            'http://aaa.com/1111', 'http://aaa.com/1112', 'http://aaa.com/1121', 'http://aaa.com/1122',
            'http://aaa.com/121', 'http://aaa.com/122',
            'http://aaa.com/1211', 'http://aaa.com/1212', 'http://aaa.com/1221', 'http://aaa.com/1222'
        ],
        "SEQUENCE_01_C": [
            'http://aaa.com/1',
            'http://aaa.com/11', 'http://aaa.com/12',
            'http://aaa.com/111', 'http://aaa.com/112', 'http://aaa.com/121', 'http://aaa.com/122',
            'http://aaa.com/1111', 'http://aaa.com/1112', 'http://aaa.com/1121', 'http://aaa.com/1122', 'http://aaa.com/1211', 'http://aaa.com/1212', 'http://aaa.com/1221', 'http://aaa.com/1222'
        ],
        "SEQUENCE_02_A": [
            'http://aaa.com/1',
            'http://aaa.com/11',
            'http://aaa.com/111', 'http://aaa.com/1111', 'http://aaa.com/1112',
            'http://aaa.com/112', 'http://aaa.com/1121', 'http://aaa.com/1122',
            'http://aaa.com/12',
            'http://aaa.com/121', 'http://aaa.com/1211', 'http://aaa.com/1212',
            'http://aaa.com/122', 'http://aaa.com/1221', 'http://aaa.com/1222',
            'http://bbb.com/1',
            'http://bbb.com/11',
            'http://bbb.com/111', 'http://bbb.com/1111', 'http://bbb.com/1112',
            'http://bbb.com/112', 'http://bbb.com/1121', 'http://bbb.com/1122',
            'http://bbb.com/12',
            'http://bbb.com/121', 'http://bbb.com/1211', 'http://bbb.com/1212',
            'http://bbb.com/122', 'http://bbb.com/1221', 'http://bbb.com/1222'
        ],
        "SEQUENCE_02_B": [
            'http://aaa.com/1', 'http://bbb.com/1',
            'http://aaa.com/11', 'http://aaa.com/12',
            'http://aaa.com/111', 'http://aaa.com/112',
            'http://aaa.com/1111', 'http://aaa.com/1112', 'http://aaa.com/1121', 'http://aaa.com/1122',
            'http://aaa.com/121', 'http://aaa.com/122',
            'http://aaa.com/1211', 'http://aaa.com/1212', 'http://aaa.com/1221', 'http://aaa.com/1222',
            'http://bbb.com/11', 'http://bbb.com/12',
            'http://bbb.com/111', 'http://bbb.com/112',
            'http://bbb.com/1111', 'http://bbb.com/1112', 'http://bbb.com/1121', 'http://bbb.com/1122',
            'http://bbb.com/121', 'http://bbb.com/122',
            'http://bbb.com/1211', 'http://bbb.com/1212', 'http://bbb.com/1221', 'http://bbb.com/1222'
        ],
        "SEQUENCE_02_C": [
            'http://aaa.com/1', 'http://bbb.com/1',
            'http://aaa.com/11', 'http://aaa.com/12', 'http://bbb.com/11', 'http://bbb.com/12',
            'http://aaa.com/111', 'http://aaa.com/112', 'http://aaa.com/121', 'http://aaa.com/122', 'http://bbb.com/111',
            'http://aaa.com/1111', 'http://aaa.com/1112', 'http://aaa.com/1121', 'http://aaa.com/1122', 'http://aaa.com/1211', 'http://aaa.com/1212', 'http://aaa.com/1221', 'http://aaa.com/1222', 'http://bbb.com/1111', 'http://bbb.com/1112',
            'http://bbb.com/112', 'http://bbb.com/121', 'http://bbb.com/122',
            'http://bbb.com/1121', 'http://bbb.com/1122', 'http://bbb.com/1211', 'http://bbb.com/1212', 'http://bbb.com/1221', 'http://bbb.com/1222'
        ],
        "SEQUENCE_02_D": [
            'http://aaa.com/1', 'http://bbb.com/1',
            'http://aaa.com/11', 'http://aaa.com/12', 'http://bbb.com/11', 'http://bbb.com/12',
            'http://aaa.com/111', 'http://aaa.com/112', 'http://aaa.com/121', 'http://aaa.com/122',
            'http://bbb.com/111', 'http://bbb.com/112', 'http://bbb.com/121', 'http://bbb.com/122',
            'http://aaa.com/1111', 'http://aaa.com/1112', 'http://aaa.com/1121', 'http://aaa.com/1122', 'http://aaa.com/1211', 'http://aaa.com/1212', 'http://aaa.com/1221', 'http://aaa.com/1222',
            'http://bbb.com/1111', 'http://bbb.com/1112', 'http://bbb.com/1121', 'http://bbb.com/1122', 'http://bbb.com/1211', 'http://bbb.com/1212', 'http://bbb.com/1221', 'http://bbb.com/1222'
        ],
        "SEQUENCE_03_A": [
            'http://ccc.com/1',
            'http://ccc.com/11',
            'http://ccc.com/111', 'http://ccc.com/1111', 'http://ccc.com/11111', 'http://ccc.com/11112', 'http://ccc.com/1112', 'http://ccc.com/11121', 'http://ccc.com/11122',
            'http://ccc.com/112', 'http://ccc.com/1121', 'http://ccc.com/11211', 'http://ccc.com/11212', 'http://ccc.com/1122', 'http://ccc.com/11221', 'http://ccc.com/11222',
            'http://ccc.com/12',
            'http://ccc.com/121', 'http://ccc.com/1211', 'http://ccc.com/12111', 'http://ccc.com/12112', 'http://ccc.com/1212', 'http://ccc.com/12121', 'http://ccc.com/12122',
            'http://ccc.com/122', 'http://ccc.com/1221', 'http://ccc.com/12211', 'http://ccc.com/12212', 'http://ccc.com/1222', 'http://ccc.com/12221', 'http://ccc.com/12222'
        ],
        "SEQUENCE_03_B": [
            'http://ccc.com/1',
            'http://ccc.com/11', 'http://ccc.com/12',
            'http://ccc.com/111', 'http://ccc.com/112',
            'http://ccc.com/1111', 'http://ccc.com/1112',
            'http://ccc.com/11111', 'http://ccc.com/11112', 'http://ccc.com/11121', 'http://ccc.com/11122',
            'http://ccc.com/1121', 'http://ccc.com/1122',
            'http://ccc.com/11211', 'http://ccc.com/11212', 'http://ccc.com/11221', 'http://ccc.com/11222',
            'http://ccc.com/121', 'http://ccc.com/122',
            'http://ccc.com/1211', 'http://ccc.com/1212',
            'http://ccc.com/12111', 'http://ccc.com/12112', 'http://ccc.com/12121', 'http://ccc.com/12122',
            'http://ccc.com/1221', 'http://ccc.com/1222',
            'http://ccc.com/12211', 'http://ccc.com/12212', 'http://ccc.com/12221', 'http://ccc.com/12222'
        ],
        "SEQUENCE_03_C": [
            'http://ccc.com/1',
            'http://ccc.com/11', 'http://ccc.com/12',
            'http://ccc.com/111', 'http://ccc.com/112', 'http://ccc.com/121', 'http://ccc.com/122',
            'http://ccc.com/1111', 'http://ccc.com/1112', 'http://ccc.com/1121', 'http://ccc.com/1122', 'http://ccc.com/1211',
            'http://ccc.com/11111', 'http://ccc.com/11112', 'http://ccc.com/11121', 'http://ccc.com/11122', 'http://ccc.com/11211', 'http://ccc.com/11212', 'http://ccc.com/11221', 'http://ccc.com/11222', 'http://ccc.com/12111', 'http://ccc.com/12112',
            'http://ccc.com/1212', 'http://ccc.com/1221', 'http://ccc.com/1222',
            'http://ccc.com/12121', 'http://ccc.com/12122', 'http://ccc.com/12211', 'http://ccc.com/12212', 'http://ccc.com/12221', 'http://ccc.com/12222'
        ],
        "SEQUENCE_03_D": [
            'http://ccc.com/1',
            'http://ccc.com/11', 'http://ccc.com/12',
            'http://ccc.com/111', 'http://ccc.com/112', 'http://ccc.com/121', 'http://ccc.com/122',
            'http://ccc.com/1111', 'http://ccc.com/1112', 'http://ccc.com/1121', 'http://ccc.com/1122', 'http://ccc.com/1211', 'http://ccc.com/1212', 'http://ccc.com/1221', 'http://ccc.com/1222',
            'http://ccc.com/11111', 'http://ccc.com/11112', 'http://ccc.com/11121', 'http://ccc.com/11122', 'http://ccc.com/11211', 'http://ccc.com/11212', 'http://ccc.com/11221', 'http://ccc.com/11222',
            'http://ccc.com/12111', 'http://ccc.com/12112', 'http://ccc.com/12121', 'http://ccc.com/12122', 'http://ccc.com/12211', 'http://ccc.com/12212', 'http://ccc.com/12221', 'http://ccc.com/12222'
        ],
    }

    @pytest.mark.parametrize(
        ('site_list', 'max_next_requests', 'expected_sequence'), [

            ('SITE_01', 1, 'SEQUENCE_01_A'),
            ('SITE_01', 2, 'SEQUENCE_01_B'),
            ('SITE_01', 5, 'SEQUENCE_01_C'),
            ('SITE_01', 10, 'SEQUENCE_01_C'),
            ('SITE_01', 100, 'SEQUENCE_01_C'),

            ('SITE_02', 1, 'SEQUENCE_02_A'),
            ('SITE_02', 2, 'SEQUENCE_02_B'),
            ('SITE_02', 5, 'SEQUENCE_02_C'),
            ('SITE_02', 10, 'SEQUENCE_02_D'),
            ('SITE_02', 100, 'SEQUENCE_02_D'),

            ('SITE_03', 1, 'SEQUENCE_03_A'),
            ('SITE_03', 2, 'SEQUENCE_03_B'),
            ('SITE_03', 5, 'SEQUENCE_03_C'),
            ('SITE_03', 10, 'SEQUENCE_03_D'),
            ('SITE_03', 100, 'SEQUENCE_03_D'),
        ]
    )
    def test_sequence(self, site_list, max_next_requests, expected_sequence):
        self.assert_sequence(
            site_list=TEST_SITES[site_list],
            expected_sequence=self.EXPECTED_SEQUENCES[expected_sequence],
            max_next_requests=max_next_requests,
        )

    def get_settings(self):
        settings = super(DFSBackendTest, self).get_settings()
        settings.TEST_MODE = True
        settings.LOGGING_MANAGER_ENABLED = False
        settings.LOGGING_BACKEND_ENABLED = False
        settings.LOGGING_DEBUGGING_ENABLED = False
        settings.STRATEGY = 'tests.backends.DFSCrawlingStrategy'
        return settings


class BFSBackendTest(BackendSequenceTest):

    EXPECTED_SEQUENCES = {
        "SEQUENCE_01_A": [
            'http://aaa.com/1',
            'http://aaa.com/11', 'http://aaa.com/12',
            'http://aaa.com/111', 'http://aaa.com/112', 'http://aaa.com/121', 'http://aaa.com/122',
            'http://aaa.com/1111', 'http://aaa.com/1112', 'http://aaa.com/1121', 'http://aaa.com/1122',
            'http://aaa.com/1211', 'http://aaa.com/1212', 'http://aaa.com/1221', 'http://aaa.com/1222'
        ],
        "SEQUENCE_02_A": [
            'http://aaa.com/1', 'http://bbb.com/1',
            'http://aaa.com/11', 'http://aaa.com/12', 'http://bbb.com/11', 'http://bbb.com/12',
            'http://aaa.com/111', 'http://aaa.com/112', 'http://aaa.com/121', 'http://aaa.com/122', 'http://bbb.com/111', 'http://bbb.com/112', 'http://bbb.com/121', 'http://bbb.com/122',
            'http://aaa.com/1111', 'http://aaa.com/1112', 'http://aaa.com/1121', 'http://aaa.com/1122', 'http://aaa.com/1211', 'http://aaa.com/1212', 'http://aaa.com/1221', 'http://aaa.com/1222',
            'http://bbb.com/1111', 'http://bbb.com/1112', 'http://bbb.com/1121', 'http://bbb.com/1122', 'http://bbb.com/1211', 'http://bbb.com/1212', 'http://bbb.com/1221', 'http://bbb.com/1222'
        ],
        "SEQUENCE_03_A": [
            'http://ccc.com/1',
            'http://ccc.com/11', 'http://ccc.com/12',
            'http://ccc.com/111', 'http://ccc.com/112', 'http://ccc.com/121', 'http://ccc.com/122',
            'http://ccc.com/1111', 'http://ccc.com/1112', 'http://ccc.com/1121', 'http://ccc.com/1122', 'http://ccc.com/1211', 'http://ccc.com/1212', 'http://ccc.com/1221', 'http://ccc.com/1222',
            'http://ccc.com/11111', 'http://ccc.com/11112', 'http://ccc.com/11121', 'http://ccc.com/11122', 'http://ccc.com/11211', 'http://ccc.com/11212', 'http://ccc.com/11221', 'http://ccc.com/11222',
            'http://ccc.com/12111', 'http://ccc.com/12112', 'http://ccc.com/12121', 'http://ccc.com/12122', 'http://ccc.com/12211', 'http://ccc.com/12212', 'http://ccc.com/12221', 'http://ccc.com/12222'
        ],
    }

    @pytest.mark.parametrize(
        ('site_list', 'max_next_requests', 'expected_sequence'), [

            ('SITE_01', 1, 'SEQUENCE_01_A'),
            ('SITE_01', 2, 'SEQUENCE_01_A'),
            ('SITE_01', 5, 'SEQUENCE_01_A'),
            ('SITE_01', 10, 'SEQUENCE_01_A'),
            ('SITE_01', 100, 'SEQUENCE_01_A'),

            ('SITE_02', 1, 'SEQUENCE_02_A'),
            ('SITE_02', 2, 'SEQUENCE_02_A'),
            ('SITE_02', 5, 'SEQUENCE_02_A'),
            ('SITE_02', 10, 'SEQUENCE_02_A'),
            ('SITE_02', 100, 'SEQUENCE_02_A'),

            ('SITE_03', 1, 'SEQUENCE_03_A'),
            ('SITE_03', 2, 'SEQUENCE_03_A'),
            ('SITE_03', 5, 'SEQUENCE_03_A'),
            ('SITE_03', 10, 'SEQUENCE_03_A'),
            ('SITE_03', 100, 'SEQUENCE_03_A'),
        ]
    )
    def test_sequence(self, site_list, max_next_requests, expected_sequence):
        self.assert_sequence(
            site_list=TEST_SITES[site_list],
            expected_sequence=self.EXPECTED_SEQUENCES[expected_sequence],
            max_next_requests=max_next_requests,
        )
    def get_settings(self):
        settings = super(BFSBackendTest, self).get_settings()
        settings.TEST_MODE = True
        settings.LOGGING_MANAGER_ENABLED = False
        settings.LOGGING_BACKEND_ENABLED = False
        settings.LOGGING_DEBUGGING_ENABLED = False
        settings.STRATEGY = 'tests.backends.BFSCrawlingStrategy'
        return settings


class RANDOMBackendTest(BackendSequenceTest):

    @pytest.mark.parametrize(
        ('site_list', 'max_next_requests'), [

            ('SITE_01', 1),
            ('SITE_01', 2),
            ('SITE_01', 5),
            ('SITE_01', 10),
            ('SITE_01', 100),

            ('SITE_02', 1),
            ('SITE_02', 2),
            ('SITE_02', 5),
            ('SITE_02', 10),
            ('SITE_02', 100),

            ('SITE_03', 1),
            ('SITE_03', 2),
            ('SITE_03', 5),
            ('SITE_03', 10),
            ('SITE_03', 100),
        ]
    )
    def test_sequence(self, site_list, max_next_requests):
        sequence = self.get_url_sequence(
            site_list=TEST_SITES[site_list],
            max_next_requests=max_next_requests,
        )
        assert len(sequence) == len(TEST_SITES[site_list])
