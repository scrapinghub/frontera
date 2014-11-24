import os
import unittest

from crawlfrontier import FrontierManager, Settings, FrontierTester, graphs


class TestParameters(object):
    add_all_pages = False
    required_attributes = [
        'site_list',
        'max_next_pages',
        'site_list',
        'add_all_pages',
    ]

    def __init__(self):
        for required_attribute in self.required_attributes:
            assert getattr(self, required_attribute, None) is not None, "Missing attribute %s" % required_attribute


#-----------------------------------------------------
# FIFO Tests Parameters
#-----------------------------------------------------
class FIFO_T01_W1(TestParameters):
    site_list = graphs.data.SITE_LIST_01
    max_next_pages = 1
    expected_sequence = [
        'A1',
        'A11', 'A12',
        'A111', 'A112', 'A121', 'A122',
        'A1111', 'A1112', 'A1121', 'A1122', 'A1211', 'A1212', 'A1221', 'A1222'
    ]


class FIFO_T01_W100(TestParameters):
    site_list = graphs.data.SITE_LIST_01
    max_next_pages = 100
    expected_sequence = [
        'A1',
        'A11', 'A12',
        'A111', 'A112', 'A121', 'A122',
        'A1111', 'A1112', 'A1121', 'A1122', 'A1211', 'A1212', 'A1221', 'A1222'
    ]


class FIFO_T01_W100_ALL(TestParameters):
    site_list = graphs.data.SITE_LIST_01
    max_next_pages = 100
    add_all_pages = True
    expected_sequence = [
        'A1',
        'A11', 'A12',
        'A111', 'A112', 'A121', 'A122',
        'A1111', 'A1112', 'A1121', 'A1122', 'A1211', 'A1212', 'A1221', 'A1222'
    ]


class FIFO_T02_W1(TestParameters):
    site_list = graphs.data.SITE_LIST_02
    max_next_pages = 1
    expected_sequence = [
        'A1', 'B1',
        'A11', 'A12', 'B11', 'B12',
        'A111', 'A112', 'A121', 'A122', 'B111', 'B112', 'B121', 'B122',
        'A1111', 'A1112', 'A1121', 'A1122', 'A1211', 'A1212', 'A1221', 'A1222',
        'B1111', 'B1112', 'B1121', 'B1122', 'B1211', 'B1212', 'B1221', 'B1222'
    ]


class FIFO_T02_W100(TestParameters):
    site_list = graphs.data.SITE_LIST_02
    max_next_pages = 100
    expected_sequence = [
        'A1', 'B1',
        'A11', 'A12', 'B11', 'B12',
        'A111', 'A112', 'A121', 'A122', 'B111', 'B112', 'B121', 'B122',
        'A1111', 'A1112', 'A1121', 'A1122', 'A1211', 'A1212', 'A1221', 'A1222',
        'B1111', 'B1112', 'B1121', 'B1122', 'B1211', 'B1212', 'B1221', 'B1222'
    ]


class FIFO_T02_W100_ALL(TestParameters):
    site_list = graphs.data.SITE_LIST_02
    max_next_pages = 100
    add_all_pages = True
    expected_sequence = [
        'A1', 'A11', 'A12', 'A111', 'A112', 'A121', 'A122',
        'A1111', 'A1112', 'A1121', 'A1122', 'A1211', 'A1212', 'A1221', 'A1222',
        'B1', 'B11', 'B12', 'B111', 'B112', 'B121', 'B122',
        'B1111', 'B1112', 'B1121', 'B1122', 'B1211', 'B1212', 'B1221', 'B1222'
    ]


class FIFO_T03_W1(TestParameters):
    site_list = graphs.data.SITE_LIST_03
    max_next_pages = 1
    expected_sequence = [
        'C1',
        'C11', 'C12',
        'C111', 'C112', 'C121', 'C122',
        'C1111', 'C1112', 'C1121', 'C1122', 'C1211', 'C1212', 'C1221', 'C1222',
        'C11111', 'C11112', 'C11121', 'C11122', 'C11211', 'C11212', 'C11221', 'C11222',
        'C12111', 'C12112', 'C12121', 'C12122', 'C12211', 'C12212', 'C12221', 'C12222'
    ]


class FIFO_T03_W100(TestParameters):
    site_list = graphs.data.SITE_LIST_03
    max_next_pages = 100
    expected_sequence = [
        'C1',
        'C11', 'C12',
        'C111', 'C112', 'C121', 'C122',
        'C1111', 'C1112', 'C1121', 'C1122', 'C1211', 'C1212', 'C1221', 'C1222',
        'C11111', 'C11112', 'C11121', 'C11122', 'C11211', 'C11212', 'C11221', 'C11222',
        'C12111', 'C12112', 'C12121', 'C12122', 'C12211', 'C12212', 'C12221', 'C12222'
    ]


#-----------------------------------------------------
# LIFO Tests Parameters
#-----------------------------------------------------
class LIFO_T01_W1(TestParameters):
    site_list = graphs.data.SITE_LIST_01
    max_next_pages = 1
    expected_sequence = [
        'A1',
        'A12', 'A122', 'A1222', 'A1221', 'A121', 'A1212', 'A1211',
        'A11', 'A112', 'A1122', 'A1121', 'A111', 'A1112', 'A1111'
    ]


class LIFO_T01_W100(TestParameters):
    site_list = graphs.data.SITE_LIST_01
    max_next_pages = 100
    expected_sequence = [
        'A1',
        'A12', 'A11',
        'A112', 'A111', 'A122', 'A121',
        'A1212', 'A1211', 'A1222', 'A1221', 'A1112', 'A1111', 'A1122', 'A1121'
    ]


class LIFO_T01_W100_ALL(TestParameters):
    site_list = graphs.data.SITE_LIST_01
    max_next_pages = 100
    add_all_pages = True
    expected_sequence = [
        'A1222', 'A1221', 'A1212', 'A1211', 'A1122', 'A1121', 'A1112', 'A1111',
        'A122', 'A121', 'A112', 'A111',
        'A12', 'A11',
        'A1'
    ]


class LIFO_T02_W1(TestParameters):
    site_list = graphs.data.SITE_LIST_02
    max_next_pages = 1
    expected_sequence = [
        'B1', 'B12', 'B122', 'B1222', 'B1221', 'B121', 'B1212', 'B1211',
        'B11', 'B112', 'B1122', 'B1121', 'B111', 'B1112', 'B1111',
        'A1', 'A12', 'A122', 'A1222', 'A1221', 'A121', 'A1212', 'A1211',
        'A11', 'A112', 'A1122', 'A1121', 'A111', 'A1112', 'A1111'
    ]


class LIFO_T02_W100(TestParameters):
    site_list = graphs.data.SITE_LIST_02
    max_next_pages = 100
    expected_sequence = [
        'B1', 'A1',
        'A12', 'A11', 'B12', 'B11',
        'B112', 'B111', 'B122', 'B121', 'A112', 'A111', 'A122', 'A121',
        'A1212', 'A1211', 'A1222', 'A1221', 'A1112', 'A1111', 'A1122', 'A1121',
        'B1212', 'B1211', 'B1222', 'B1221', 'B1112', 'B1111', 'B1122', 'B1121'
    ]


class LIFO_T02_W100_ALL(TestParameters):
    site_list = graphs.data.SITE_LIST_02
    max_next_pages = 100
    add_all_pages = True
    expected_sequence = [
        'B1222', 'B1221', 'B1212', 'B1211', 'B1122', 'B1121', 'B1112', 'B1111',
        'B122', 'B121', 'B112', 'B111',
        'B12', 'B11',
        'B1',
        'A1222', 'A1221', 'A1212', 'A1211', 'A1122', 'A1121', 'A1112', 'A1111',
        'A122', 'A121', 'A112', 'A111',
        'A12', 'A11',
        'A1'
    ]


class LIFO_T03_W1(TestParameters):
    site_list = graphs.data.SITE_LIST_03
    max_next_pages = 1
    expected_sequence = [
        'C1', 'C12', 'C122', 'C1222', 'C12222', 'C12221', 'C1221', 'C12212', 'C12211',
        'C121', 'C1212', 'C12122', 'C12121', 'C1211', 'C12112', 'C12111',
        'C11', 'C112', 'C1122', 'C11222', 'C11221', 'C1121', 'C11212', 'C11211',
        'C111', 'C1112', 'C11122', 'C11121', 'C1111', 'C11112', 'C11111'
    ]


class LIFO_T03_W100(TestParameters):
    site_list = graphs.data.SITE_LIST_03
    max_next_pages = 100
    expected_sequence = [
        'C1',
        'C12', 'C11',
        'C112', 'C111', 'C122', 'C121',
        'C1212', 'C1211', 'C1222', 'C1221', 'C1112', 'C1111', 'C1122', 'C1121',
        'C11212', 'C11211', 'C11222', 'C11221', 'C11112', 'C11111', 'C11122', 'C11121',
        'C12212', 'C12211', 'C12222', 'C12221', 'C12112', 'C12111', 'C12122', 'C12121'
    ]
    

#-----------------------------------------------------
# DFS Tests Parameters
#-----------------------------------------------------
class DFS_T01_W1(TestParameters):
    site_list = graphs.data.SITE_LIST_01
    max_next_pages = 1
    expected_sequence = [
        'A1',
        'A11', 'A111', 'A1111', 'A1112', 'A112', 'A1121', 'A1122',
        'A12', 'A121', 'A1211', 'A1212', 'A122', 'A1221', 'A1222'
    ]


class DFS_T01_W100(TestParameters):
    site_list = graphs.data.SITE_LIST_01
    max_next_pages = 100
    expected_sequence = [
        'A1',
        'A11', 'A12',
        'A111', 'A112', 'A121', 'A122',
        'A1111', 'A1112', 'A1121', 'A1122', 'A1211', 'A1212', 'A1221', 'A1222'
    ]


class DFS_T02_W1(TestParameters):
    site_list = graphs.data.SITE_LIST_02
    max_next_pages = 1
    expected_sequence = [
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
    ]


class DFS_T02_W100(TestParameters):
    site_list = graphs.data.SITE_LIST_02
    max_next_pages = 100
    expected_sequence = [
        'A1', 'B1',
        'A11', 'A12', 'B11', 'B12',
        'A111', 'A112', 'A121', 'A122',
        'B111', 'B112', 'B121', 'B122',
        'A1111', 'A1112', 'A1121', 'A1122', 'A1211', 'A1212', 'A1221', 'A1222',
        'B1111', 'B1112', 'B1121', 'B1122', 'B1211', 'B1212', 'B1221', 'B1222'
    ]


class DFS_T03_W1(TestParameters):
    site_list = graphs.data.SITE_LIST_03
    max_next_pages = 1
    expected_sequence = [
        'C1',
        'C11',
        'C111', 'C1111', 'C11111', 'C11112', 'C1112', 'C11121', 'C11122',
        'C112', 'C1121', 'C11211', 'C11212', 'C1122', 'C11221', 'C11222',
        'C12',
        'C121', 'C1211', 'C12111', 'C12112', 'C1212', 'C12121', 'C12122',
        'C122', 'C1221', 'C12211', 'C12212', 'C1222', 'C12221', 'C12222'
    ]


class DFS_T03_W100(TestParameters):
    site_list = graphs.data.SITE_LIST_03
    max_next_pages = 100
    expected_sequence = [
        'C1',
        'C11', 'C12',
        'C111', 'C112', 'C121', 'C122',
        'C1111', 'C1112', 'C1121', 'C1122', 'C1211', 'C1212', 'C1221', 'C1222',
        'C11111', 'C11112', 'C11121', 'C11122', 'C11211', 'C11212', 'C11221', 'C11222',
        'C12111', 'C12112', 'C12121', 'C12122', 'C12211', 'C12212', 'C12221', 'C12222'
    ]


#-----------------------------------------------------
# BFS Tests Parameters
#-----------------------------------------------------
class BFS_T01_W1(TestParameters):
    site_list = graphs.data.SITE_LIST_01
    max_next_pages = 1
    expected_sequence = [
        'A1',
        'A11', 'A12',
        'A111', 'A112', 'A121', 'A122',
        'A1111', 'A1112', 'A1121', 'A1122',
        'A1211', 'A1212', 'A1221', 'A1222'
    ]


class BFS_T01_W100(TestParameters):
    site_list = graphs.data.SITE_LIST_01
    max_next_pages = 100
    expected_sequence = [
        'A1',
        'A11', 'A12',
        'A111', 'A112', 'A121', 'A122',
        'A1111', 'A1112', 'A1121', 'A1122',
        'A1211', 'A1212', 'A1221', 'A1222'
    ]


class BFS_T01_W100_ALL(TestParameters):
    site_list = graphs.data.SITE_LIST_01
    max_next_pages = 100
    add_all_pages = True
    expected_sequence = [
        'A1',
        'A11', 'A12',
        'A111', 'A112', 'A121', 'A122',
        'A1111', 'A1112', 'A1121', 'A1122',
        'A1211', 'A1212', 'A1221', 'A1222'
    ]


class BFS_T02_W1(TestParameters):
    site_list = graphs.data.SITE_LIST_02
    max_next_pages = 1
    expected_sequence = [
        'A1', 'B1',
        'A11', 'A12', 'B11', 'B12',
        'A111', 'A112', 'A121', 'A122',
        'B111', 'B112', 'B121', 'B122',
        'A1111', 'A1112', 'A1121', 'A1122', 'A1211', 'A1212', 'A1221', 'A1222',
        'B1111', 'B1112', 'B1121', 'B1122', 'B1211', 'B1212', 'B1221', 'B1222'
    ]


class BFS_T02_W100(TestParameters):
    site_list = graphs.data.SITE_LIST_02
    max_next_pages = 100
    expected_sequence = [
        'A1', 'B1',
        'A11', 'A12', 'B11', 'B12',
        'A111', 'A112', 'A121', 'A122',
        'B111', 'B112', 'B121', 'B122',
        'A1111', 'A1112', 'A1121', 'A1122', 'A1211', 'A1212', 'A1221', 'A1222',
        'B1111', 'B1112', 'B1121', 'B1122', 'B1211', 'B1212', 'B1221', 'B1222'
    ]


class BFS_T03_W1(TestParameters):
    site_list = graphs.data.SITE_LIST_03
    max_next_pages = 1
    expected_sequence = [
        'C1',
        'C11', 'C12',
        'C111', 'C112', 'C121', 'C122',
        'C1111', 'C1112', 'C1121', 'C1122', 'C1211', 'C1212', 'C1221', 'C1222',
        'C11111', 'C11112', 'C11121', 'C11122', 'C11211', 'C11212', 'C11221', 'C11222',
        'C12111', 'C12112', 'C12121', 'C12122', 'C12211', 'C12212', 'C12221', 'C12222'
    ]


class BFS_T03_W100(TestParameters):
    site_list = graphs.data.SITE_LIST_03
    max_next_pages = 100
    expected_sequence = [
        'C1',
        'C11', 'C12',
        'C111', 'C112', 'C121', 'C122',
        'C1111', 'C1112', 'C1121', 'C1122', 'C1211', 'C1212', 'C1221', 'C1222',
        'C11111', 'C11112', 'C11121', 'C11122', 'C11211', 'C11212', 'C11221', 'C11222',
        'C12111', 'C12112', 'C12121', 'C12122', 'C12211', 'C12212', 'C12221', 'C12222'
    ]


#-----------------------------------------------------
# Backend Test Case
#-----------------------------------------------------
class BackendTestCase(unittest.TestCase):
    def setUp(self):
        # Graph
        graph_manager = graphs.Manager()
        graph_manager.add_site_list(self.test_parameters.site_list)

        # Settings
        self.settings.TEST_MODE = True
        self.settings.BACKEND = self.backend
        self.settings.LOGGING_MANAGER_ENABLED = False
        self.settings.LOGGING_BACKEND_ENABLED = False
        self.settings.LOGGING_DEBUGGING_ENABLED = False

        # Frontier
        self.frontier = FrontierManager.from_settings(self.settings)

        # Tester
        self.tester = FrontierTester(frontier=self.frontier,
                                     graph_manager=graph_manager,
                                     max_next_pages=self.test_parameters.max_next_pages)
        self.tester.run(add_all_pages=self.test_parameters.add_all_pages)

    def tearDown(self):
        if self.teardown_callbacks:
            for callback in self.teardown_callbacks:
                callback()

    def test_sequence(self):
        sequence = [page.url for page in self.tester.sequence]
        #print self.frontier.backend.name
        #print sequence
        assert len(sequence) == len(self.test_parameters.expected_sequence)  # to help preparing tests
        assert sequence == self.test_parameters.expected_sequence  # real test


class BackendTest(object):
    def __init__(self, name, backend, test_parameters, settings=None, teardown_callbacks=None):
        self.name = name
        self.backend = backend
        self.settings = settings or Settings()
        self.test_parameters = test_parameters
        self.teardown_callbacks = teardown_callbacks


#-----------------------------------------------------
# Test Parameters
#-----------------------------------------------------
FIFO_TEST_PARAMETERS = [
    FIFO_T01_W1,
    FIFO_T01_W100,
    FIFO_T01_W100_ALL,
    FIFO_T02_W1,
    FIFO_T02_W100,
    FIFO_T02_W100_ALL,
    FIFO_T03_W1,
    FIFO_T03_W100,
]

LIFO_TEST_PARAMETERS = [
    LIFO_T01_W1,
    LIFO_T01_W100,
    LIFO_T01_W100_ALL,
    LIFO_T02_W1,
    LIFO_T02_W100,
    LIFO_T02_W100_ALL,
    LIFO_T03_W1,
    LIFO_T03_W100,
]

DFS_TEST_PARAMETERS = [
    DFS_T01_W1,
    DFS_T01_W100,
    DFS_T02_W1,
    DFS_T02_W100,
    DFS_T03_W1,
    DFS_T03_W100,
]

BFS_TEST_PARAMETERS = [
    BFS_T01_W1,
    BFS_T01_W100,
    BFS_T01_W100_ALL,
    BFS_T02_W1,
    BFS_T02_W100,
    BFS_T03_W1,
    BFS_T03_W100,
]


#-----------------------------------------------------
# Sqlalchemy settings
#-----------------------------------------------------
def delete_test_db():
    os.remove(SQLALCHEMY_DB_NAME)

SQLALCHEMY_DB_NAME = 'test.db'
SQLALCHEMY_SQLITE_MEMORY_SETTINGS = Settings.from_params()
SQLALCHEMY_SQLITE_FILE_SETTINGS = Settings.from_params(SQLALCHEMYBACKEND_ENGINE='sqlite:///' + SQLALCHEMY_DB_NAME)
SQLALCHEMY_SQLITE_FILE_TEARDOWN_CALLBACKS = [delete_test_db]

#-----------------------------------------------------
# BACKEND TESTS
#-----------------------------------------------------
#-----------------------------
# memory dict
#-----------------------------
MEMORY_DICT_BACKEND_TESTS = [
    BackendTest(
        name='DICT_FIFO',
        backend='crawlfrontier.contrib.backends.memory.dict.FIFO',
        test_parameters=FIFO_TEST_PARAMETERS
    ),
    BackendTest(
        name='DICT_LIFO',
        backend='crawlfrontier.contrib.backends.memory.dict.LIFO',
        test_parameters=LIFO_TEST_PARAMETERS
    ),
    BackendTest(
        name='DICT_DFS',
        backend='crawlfrontier.contrib.backends.memory.dict.DFS',
        test_parameters=DFS_TEST_PARAMETERS
    ),
    BackendTest(
        name='DICT_BFS',
        backend='crawlfrontier.contrib.backends.memory.dict.BFS',
        test_parameters=BFS_TEST_PARAMETERS
    ),
]

#-----------------------------
# memory heapq
#-----------------------------
MEMORY_HEAPQ_BACKEND_TESTS = [
    BackendTest(
        name='HEAPQ_FIFO',
        backend='crawlfrontier.contrib.backends.memory.heapq.FIFO',
        test_parameters=FIFO_TEST_PARAMETERS
    ),
    BackendTest(
        name='HEAPQ_LIFO',
        backend='crawlfrontier.contrib.backends.memory.heapq.LIFO',
        test_parameters=LIFO_TEST_PARAMETERS
    ),
    BackendTest(
        name='HEAPQ_DFS',
        backend='crawlfrontier.contrib.backends.memory.heapq.DFS',
        test_parameters=DFS_TEST_PARAMETERS
    ),
    BackendTest(
        name='HEAPQ_BFS',
        backend='crawlfrontier.contrib.backends.memory.heapq.BFS',
        test_parameters=BFS_TEST_PARAMETERS
    ),
]

#-----------------------------
# sqlalchemy sqlite/memory
#-----------------------------
SQLALCHEMY_MEMORY_BACKEND_TESTS = [
    BackendTest(
        name='SQLALCHEMY_SQLITE_MEM_FIFO',
        backend='crawlfrontier.contrib.backends.sqlalchemy.FIFO',
        settings=SQLALCHEMY_SQLITE_MEMORY_SETTINGS,
        test_parameters=FIFO_TEST_PARAMETERS
    ),
    BackendTest(
        name='SQLALCHEMY_SQLITE_MEM_LIFO',
        backend='crawlfrontier.contrib.backends.sqlalchemy.LIFO',
        settings=SQLALCHEMY_SQLITE_MEMORY_SETTINGS,
        test_parameters=LIFO_TEST_PARAMETERS
    ),
    BackendTest(
        name='SQLALCHEMY_SQLITE_MEM_DFS',
        backend='crawlfrontier.contrib.backends.sqlalchemy.DFS',
        settings=SQLALCHEMY_SQLITE_MEMORY_SETTINGS,
        test_parameters=DFS_TEST_PARAMETERS
    ),
    BackendTest(
        name='SQLALCHEMY_SQLITE_MEM_BFS',
        backend='crawlfrontier.contrib.backends.sqlalchemy.BFS',
        settings=SQLALCHEMY_SQLITE_MEMORY_SETTINGS,
        test_parameters=BFS_TEST_PARAMETERS
    ),
]

#-----------------------------
# sqlalchemy sqlite/file
#-----------------------------
SQLALCHEMY_FILE_BACKEND_TESTS = [
    BackendTest(
        name='SQLALCHEMY_SQLITE_FILE_FIFO',
        backend='crawlfrontier.contrib.backends.sqlalchemy.FIFO',
        settings=SQLALCHEMY_SQLITE_FILE_SETTINGS,
        test_parameters=FIFO_TEST_PARAMETERS,
        teardown_callbacks=SQLALCHEMY_SQLITE_FILE_TEARDOWN_CALLBACKS
    ),
    BackendTest(
        name='SQLALCHEMY_SQLITE_FILE_LIFO',
        backend='crawlfrontier.contrib.backends.sqlalchemy.LIFO',
        settings=SQLALCHEMY_SQLITE_FILE_SETTINGS,
        test_parameters=LIFO_TEST_PARAMETERS,
        teardown_callbacks=SQLALCHEMY_SQLITE_FILE_TEARDOWN_CALLBACKS
    ),
    BackendTest(
        name='SQLALCHEMY_SQLITE_FILE_DFS',
        backend='crawlfrontier.contrib.backends.sqlalchemy.DFS',
        settings=SQLALCHEMY_SQLITE_FILE_SETTINGS,
        test_parameters=DFS_TEST_PARAMETERS,
        teardown_callbacks=SQLALCHEMY_SQLITE_FILE_TEARDOWN_CALLBACKS
    ),
    BackendTest(
        name='SQLALCHEMY_SQLITE_FILE_BFS',
        backend='crawlfrontier.contrib.backends.sqlalchemy.BFS',
        settings=SQLALCHEMY_SQLITE_FILE_SETTINGS,
        test_parameters=BFS_TEST_PARAMETERS,
        teardown_callbacks=SQLALCHEMY_SQLITE_FILE_TEARDOWN_CALLBACKS
    ),
]


BACKEND_TESTS = \
    MEMORY_DICT_BACKEND_TESTS + \
    MEMORY_HEAPQ_BACKEND_TESTS + \
    SQLALCHEMY_MEMORY_BACKEND_TESTS + \
    SQLALCHEMY_FILE_BACKEND_TESTS


#-----------------------------------------------------
# Test loading/creation
#-----------------------------------------------------
def _create_test(test_klass_name, backend_test, test_parameters):
    TestKlass = type(test_klass_name, (BackendTestCase, ), BackendTestCase.__dict__.copy())
    test = TestKlass(methodName='test_sequence')
    test.settings = backend_test.settings
    test.backend = backend_test.backend
    test.test_parameters = test_parameters
    test.teardown_callbacks = backend_test.teardown_callbacks
    return test


def load_tests(loader, tests, pattern):
    tests = []
    for backend_test in BACKEND_TESTS:
        for test_parameters in backend_test.test_parameters:
            test_klass_name = '%s_%s' % (backend_test.name, test_parameters.__name__)
            tests.append(_create_test(test_klass_name=test_klass_name,
                                      backend_test=backend_test,
                                      test_parameters=test_parameters()))
    return tests

if __name__ == '__main__':
    suite = unittest.TestSuite()
    for test in load_tests(None, None, None):
        suite.addTest(test)
    unittest.TextTestRunner().run(suite)
