=====
Tests
=====

Frontera tests are implemented using the `pytest`_ tool.

You can install `pytest`_ and the additional required libraries used in the tests using pip::

    pip install -r requirements/tests.txt


Running tests
=============

To run all tests go to the root directory of source code and run::

    py.test


Writing tests
=============

All functionality (including new features and bug fixes) must include a test case to check that it works as expected,
so please include tests for your patches if you want them to get accepted sooner.


Backend testing
===============

A base `pytest`_ class for :class:`Backend <frontera.core.components.Backend>` testing is provided:
:class:`BackendTest <frontera.tests.backends.BackendTest>`

.. autoclass:: frontera.tests.backends.BackendTest

    .. automethod:: frontera.tests.backends.BackendTest.get_settings
    .. automethod:: frontera.tests.backends.BackendTest.get_frontier
    .. automethod:: frontera.tests.backends.BackendTest.setup_backend
    .. automethod:: frontera.tests.backends.BackendTest.teardown_backend


Let's say for instance that you want to test to your backend ``MyBackend`` and create a new frontier instance for each
test method call, you can define a test class like this::


    class TestMyBackend(backends.BackendTest):

        backend_class = 'frontera.contrib.backend.abackend.MyBackend'

        def test_one(self):
            frontier = self.get_frontier()
            ...

        def test_two(self):
            frontier = self.get_frontier()
            ...

        ...


And let's say too that it uses a database file and you need to clean it before and after each test::


    class TestMyBackend(backends.BackendTest):

        backend_class = 'frontera.contrib.backend.abackend.MyBackend'

        def setup_backend(self, method):
            self._delete_test_db()

        def teardown_backend(self, method):
            self._delete_test_db()

        def _delete_test_db(self):
            try:
                os.remove('mytestdb.db')
            except OSError:
                pass

        def test_one(self):
            frontier = self.get_frontier()
            ...

        def test_two(self):
            frontier = self.get_frontier()
            ...

        ...


Testing backend sequences
=========================

To test :class:`Backend <frontera.core.components.Backend>` crawling sequences you can use the
:class:`BackendSequenceTest <frontera.tests.backends.BackendSequenceTest>` class.

.. autoclass:: frontera.tests.backends.BackendSequenceTest

    .. automethod:: frontera.tests.backends.BackendSequenceTest.get_sequence
    .. automethod:: frontera.tests.backends.BackendSequenceTest.assert_sequence


:class:`BackendSequenceTest <frontera.tests.backends.BackendSequenceTest>` class will run a complete crawl of the passed
site graphs and return the sequence used by the backend for visiting the different pages.

Let's say you want to test to a backend that sort pages using alphabetic order.
You can define the following test::


    class TestAlphabeticSortBackend(backends.BackendSequenceTest):

        backend_class = 'frontera.contrib.backend.abackend.AlphabeticSortBackend'

        SITE_LIST = [
            [
                ('C', []),
                ('B', []),
                ('A', []),
            ],
        ]

        def test_one(self):
            # Check sequence is the expected one
            self.assert_sequence(site_list=self.SITE_LIST,
                                 expected_sequence=['A', 'B', 'C'],
                                 max_next_requests=0)

        def test_two(self):
            # Get sequence and work with it
            sequence = self.get_sequence(site_list=SITE_LIST,
                                max_next_requests=0)
            assert len(sequence) > 2

        ...


Testing basic algorithms
========================

If your backend uses any of the :ref:`basic algorithms logics <frontier-backends-basic-algorithms>`, you can just
inherit the correponding test base class for each logic and sequences will be automatically tested for it::

    from frontera.tests import backends


    class TestMyBackendFIFO(backends.FIFOBackendTest):
        backend_class = 'frontera.contrib.backends.abackend.MyBackendFIFO'


    class TestMyBackendLIFO(backends.LIFOBackendTest):
        backend_class = 'frontera.contrib.backends.abackend.MyBackendLIFO'


    class TestMyBackendDFS(backends.DFSBackendTest):
        backend_class = 'frontera.contrib.backends.abackend.MyBackendDFS'


    class TestMyBackendBFS(backends.BFSBackendTest):
        backend_class = 'frontera.contrib.backends.abackend.MyBackendBFS'


    class TestMyBackendRANDOM(backends.RANDOMBackendTest):
        backend_class = 'frontera.contrib.backends.abackend.MyBackendRANDOM'



.. _pytest: http://pytest.org/latest/

