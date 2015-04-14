import pytest

from frontera import FrontierManager, Settings, FrontierTester, graphs
from frontera.utils.tester import DownloaderSimulator, BaseDownloaderSimulator


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
        return FrontierManager.from_settings(self.get_settings())

    def get_settings(self):
        """
        Returns backend settings
        """
        return Settings(attributes={
            'BACKEND': self.backend_class
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

    def get_sequence(self, site_list, max_next_requests, downloader_simulator=BaseDownloaderSimulator()):
        """
        Returns a crawling sequence from a site list

        :param list site_list: A list of sites to use as frontier seeds.
        :param int max_next_requests: Max next requests for the frontier.
        """

        # Graph
        graph_manager = graphs.Manager()
        graph_manager.add_site_list(site_list)

        # Tester
        tester = FrontierTester(frontier=self.get_frontier(),
                                graph_manager=graph_manager,
                                max_next_requests=max_next_requests,
                                downloader_simulator=downloader_simulator)

        # Run tester and generate sequence
        tester.run()
        return [page.url for page in tester.sequence]

    def assert_sequence(self, site_list, expected_sequence, max_next_requests):
        """
        Asserts that crawling sequence is the one expected

        :param list site_list: A list of sites to use as frontier seeds.
        :param int max_next_requests: Max next requests for the frontier.
        """

        # Get sequence
        sequence = self.get_sequence(site_list, max_next_requests)
        #print [str(n) for n in sequence]

        # Assert sequence equals expected
        assert len(sequence) == len(expected_sequence)
        assert sequence == expected_sequence


class FIFOBackendTest(BackendSequenceTest):

    EXPECTED_SEQUENCES = {
        "SEQUENCE_01_A": [
            'A1',
            'A11', 'A12',
            'A111', 'A112', 'A121', 'A122',
            'A1111', 'A1112', 'A1121', 'A1122', 'A1211', 'A1212', 'A1221', 'A1222'
        ],
        "SEQUENCE_02_A": [
            'A1', 'B1',
            'A11', 'A12', 'B11', 'B12',
            'A111', 'A112', 'A121', 'A122', 'B111', 'B112', 'B121', 'B122',
            'A1111', 'A1112', 'A1121', 'A1122', 'A1211', 'A1212', 'A1221', 'A1222',
            'B1111', 'B1112', 'B1121', 'B1122', 'B1211', 'B1212', 'B1221', 'B1222'
        ],
        "SEQUENCE_03_A": [
            'C1',
            'C11', 'C12',
            'C111', 'C112', 'C121', 'C122',
            'C1111', 'C1112', 'C1121', 'C1122', 'C1211', 'C1212', 'C1221', 'C1222',
            'C11111', 'C11112', 'C11121', 'C11122', 'C11211', 'C11212', 'C11221', 'C11222',
            'C12111', 'C12112', 'C12121', 'C12122', 'C12211', 'C12212', 'C12221', 'C12222'
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

    def test_sequence1(self):
        sequence = self.get_sequence(TEST_SITES['SITE_09'], max_next_requests=5,
                                     downloader_simulator=DownloaderSimulator(rate=1))

        expected_sequence = self.EXPECTED_SEQUENCES['SEQUENCE_01_A']
        assert len(sequence) == len(expected_sequence)
        assert sequence == expected_sequence

    def test_sequence2(self):
        sequence = self.get_sequence(TEST_SITES['SITE_09'], max_next_requests=5,
                                     downloader_simulator=BaseDownloaderSimulator())

        expected_sequence = self.EXPECTED_SEQUENCES['SEQUENCE_02_A']
        assert len(sequence) == len(expected_sequence)
        assert sequence == expected_sequence


class LIFOBackendTest(BackendSequenceTest):

    EXPECTED_SEQUENCES = {
        "SEQUENCE_01_A": [
            'A1',
            'A12',
            'A122', 'A1222', 'A1221',
            'A121', 'A1212', 'A1211',
            'A11',
            'A112', 'A1122', 'A1121',
            'A111', 'A1112', 'A1111'
        ],
        "SEQUENCE_01_B": [
            'A1',
            'A12', 'A11',
            'A112', 'A111',
            'A1112', 'A1111', 'A1122', 'A1121',
            'A122', 'A121',
            'A1212', 'A1211', 'A1222', 'A1221'],
        "SEQUENCE_01_C": [
            'A1',
            'A12', 'A11',
            'A112', 'A111', 'A122', 'A121',
            'A1212', 'A1211', 'A1222', 'A1221', 'A1112', 'A1111', 'A1122', 'A1121'
        ],
        "SEQUENCE_02_A": [
            'B1',
            'B12', 'B122', 'B1222', 'B1221', 'B121', 'B1212', 'B1211',
            'B11', 'B112', 'B1122', 'B1121', 'B111', 'B1112', 'B1111',
            'A1',
            'A12', 'A122', 'A1222', 'A1221', 'A121', 'A1212', 'A1211',
            'A11', 'A112', 'A1122', 'A1121', 'A111', 'A1112', 'A1111'
        ],
        "SEQUENCE_02_B": [
            'B1', 'A1',
            'A12', 'A11',
            'A112', 'A111',
            'A1112', 'A1111', 'A1122', 'A1121',
            'A122', 'A121',
            'A1212', 'A1211', 'A1222', 'A1221',
            'B12', 'B11',
            'B112', 'B111',
            'B1112', 'B1111', 'B1122', 'B1121',
            'B122', 'B121',
            'B1212', 'B1211', 'B1222', 'B1221'
        ],
        "SEQUENCE_02_C": [
            'B1', 'A1',
            'A12', 'A11', 'B12', 'B11', 'B112', 'B111', 'B122', 'B121', 'A112',
            'A1122', 'A1121', 'B1212', 'B1211', 'B1222', 'B1221', 'B1112', 'B1111', 'B1122', 'B1121',
            'A111', 'A122', 'A121',
            'A1212', 'A1211', 'A1222', 'A1221', 'A1112', 'A1111'
        ],
        "SEQUENCE_02_D": [
            'B1', 'A1',
            'A12', 'A11', 'B12', 'B11', 'B112', 'B111', 'B122', 'B121', 'A112', 'A111', 'A122', 'A121',
            'A1212', 'A1211', 'A1222', 'A1221', 'A1112', 'A1111', 'A1122', 'A1121',
            'B1212', 'B1211', 'B1222', 'B1221', 'B1112', 'B1111', 'B1122', 'B1121'
        ],

        "SEQUENCE_03_A": [
            'C1', 'C12', 'C122', 'C1222', 'C12222', 'C12221', 'C1221', 'C12212', 'C12211',
            'C121', 'C1212', 'C12122', 'C12121', 'C1211', 'C12112', 'C12111',
            'C11', 'C112', 'C1122', 'C11222', 'C11221', 'C1121', 'C11212', 'C11211',
            'C111', 'C1112', 'C11122', 'C11121', 'C1111', 'C11112', 'C11111'
        ],
        "SEQUENCE_03_B": [
            'C1',
            'C12', 'C11',
            'C112', 'C111',
            'C1112', 'C1111', 'C11112', 'C11111', 'C11122', 'C11121',
            'C1122', 'C1121', 'C11212', 'C11211', 'C11222', 'C11221',
            'C122', 'C121',
            'C1212', 'C1211', 'C12112', 'C12111', 'C12122', 'C12121',
            'C1222', 'C1221', 'C12212', 'C12211', 'C12222', 'C12221'
        ],
        "SEQUENCE_03_C": [
            'C1',
            'C12', 'C11',
            'C112', 'C111', 'C122', 'C121',
            'C1212', 'C1211', 'C1222', 'C1221', 'C1112',
            'C11122', 'C11121', 'C12212', 'C12211',
            'C12222', 'C12221', 'C12112', 'C12111',
            'C12122', 'C12121',
            'C1111', 'C1122', 'C1121', 'C11212',
            'C11211', 'C11222', 'C11221', 'C11112', 'C11111'
        ],
        "SEQUENCE_03_D": [
            'C1',
            'C12', 'C11',
            'C112', 'C111', 'C122', 'C121',
            'C1212', 'C1211', 'C1222', 'C1221',
            'C1112', 'C1111', 'C1122', 'C1121',
            'C11212', 'C11211', 'C11222', 'C11221', 'C11112', 'C11111', 'C11122', 'C11121',
            'C12212', 'C12211', 'C12222', 'C12221', 'C12112', 'C12111', 'C12122', 'C12121'
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
            'A1',
            'A11', 'A111', 'A1111', 'A1112', 'A112', 'A1121', 'A1122',
            'A12', 'A121', 'A1211', 'A1212', 'A122', 'A1221', 'A1222'
        ],
        "SEQUENCE_01_B": [
            'A1',
            'A11', 'A12',
            'A111', 'A112',
            'A1111', 'A1112', 'A1121', 'A1122',
            'A121', 'A122',
            'A1211', 'A1212', 'A1221', 'A1222'
        ],
        "SEQUENCE_01_C": [
            'A1',
            'A11', 'A12',
            'A111', 'A112', 'A121', 'A122',
            'A1111', 'A1112', 'A1121', 'A1122', 'A1211', 'A1212', 'A1221', 'A1222'
        ],
        "SEQUENCE_02_A": [
            'A1',
            'A11',
            'A111', 'A1111', 'A1112',
            'A112', 'A1121', 'A1122',
            'A12',
            'A121', 'A1211', 'A1212',
            'A122', 'A1221', 'A1222',
            'B1',
            'B11',
            'B111', 'B1111', 'B1112',
            'B112', 'B1121', 'B1122',
            'B12',
            'B121', 'B1211', 'B1212',
            'B122', 'B1221', 'B1222'
        ],
        "SEQUENCE_02_B": [
            'A1', 'B1',
            'A11', 'A12',
            'A111', 'A112',
            'A1111', 'A1112', 'A1121', 'A1122',
            'A121', 'A122',
            'A1211', 'A1212', 'A1221', 'A1222',
            'B11', 'B12',
            'B111', 'B112',
            'B1111', 'B1112', 'B1121', 'B1122',
            'B121', 'B122',
            'B1211', 'B1212', 'B1221', 'B1222'
        ],
        "SEQUENCE_02_C": [
            'A1', 'B1',
            'A11', 'A12', 'B11', 'B12',
            'A111', 'A112', 'A121', 'A122', 'B111',
            'A1111', 'A1112', 'A1121', 'A1122', 'A1211', 'A1212', 'A1221', 'A1222', 'B1111', 'B1112',
            'B112', 'B121', 'B122',
            'B1121', 'B1122', 'B1211', 'B1212', 'B1221', 'B1222'
        ],
        "SEQUENCE_02_D": [
            'A1', 'B1',
            'A11', 'A12', 'B11', 'B12',
            'A111', 'A112', 'A121', 'A122',
            'B111', 'B112', 'B121', 'B122',
            'A1111', 'A1112', 'A1121', 'A1122', 'A1211', 'A1212', 'A1221', 'A1222',
            'B1111', 'B1112', 'B1121', 'B1122', 'B1211', 'B1212', 'B1221', 'B1222'
        ],
        "SEQUENCE_03_A": [
            'C1',
            'C11',
            'C111', 'C1111', 'C11111', 'C11112', 'C1112', 'C11121', 'C11122',
            'C112', 'C1121', 'C11211', 'C11212', 'C1122', 'C11221', 'C11222',
            'C12',
            'C121', 'C1211', 'C12111', 'C12112', 'C1212', 'C12121', 'C12122',
            'C122', 'C1221', 'C12211', 'C12212', 'C1222', 'C12221', 'C12222'
        ],
        "SEQUENCE_03_B": [
            'C1',
            'C11', 'C12',
            'C111', 'C112',
            'C1111', 'C1112',
            'C11111', 'C11112', 'C11121', 'C11122',
            'C1121', 'C1122',
            'C11211', 'C11212', 'C11221', 'C11222',
            'C121', 'C122',
            'C1211', 'C1212',
            'C12111', 'C12112', 'C12121', 'C12122',
            'C1221', 'C1222',
            'C12211', 'C12212', 'C12221', 'C12222'
        ],
        "SEQUENCE_03_C": [
            'C1',
            'C11', 'C12',
            'C111', 'C112', 'C121', 'C122',
            'C1111', 'C1112', 'C1121', 'C1122', 'C1211',
            'C11111', 'C11112', 'C11121', 'C11122', 'C11211', 'C11212', 'C11221', 'C11222', 'C12111', 'C12112',
            'C1212', 'C1221', 'C1222',
            'C12121', 'C12122', 'C12211', 'C12212', 'C12221', 'C12222'
        ],
        "SEQUENCE_03_D": [
            'C1',
            'C11', 'C12',
            'C111', 'C112', 'C121', 'C122',
            'C1111', 'C1112', 'C1121', 'C1122', 'C1211', 'C1212', 'C1221', 'C1222',
            'C11111', 'C11112', 'C11121', 'C11122', 'C11211', 'C11212', 'C11221', 'C11222',
            'C12111', 'C12112', 'C12121', 'C12122', 'C12211', 'C12212', 'C12221', 'C12222'
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


class BFSBackendTest(BackendSequenceTest):

    EXPECTED_SEQUENCES = {
        "SEQUENCE_01_A": [
            'A1',
            'A11', 'A12',
            'A111', 'A112', 'A121', 'A122',
            'A1111', 'A1112', 'A1121', 'A1122',
            'A1211', 'A1212', 'A1221', 'A1222'
        ],
        "SEQUENCE_02_A": [
            'A1', 'B1',
            'A11', 'A12', 'B11', 'B12',
            'A111', 'A112', 'A121', 'A122', 'B111', 'B112', 'B121', 'B122',
            'A1111', 'A1112', 'A1121', 'A1122', 'A1211', 'A1212', 'A1221', 'A1222',
            'B1111', 'B1112', 'B1121', 'B1122', 'B1211', 'B1212', 'B1221', 'B1222'
        ],
        "SEQUENCE_03_A": [
            'C1',
            'C11', 'C12',
            'C111', 'C112', 'C121', 'C122',
            'C1111', 'C1112', 'C1121', 'C1122', 'C1211', 'C1212', 'C1221', 'C1222',
            'C11111', 'C11112', 'C11121', 'C11122', 'C11211', 'C11212', 'C11221', 'C11222',
            'C12111', 'C12112', 'C12121', 'C12122', 'C12211', 'C12212', 'C12221', 'C12222'
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
        sequence = self.get_sequence(
            site_list=TEST_SITES[site_list],
            max_next_requests=max_next_requests,
        )
        assert len(sequence) == len(TEST_SITES[site_list])
