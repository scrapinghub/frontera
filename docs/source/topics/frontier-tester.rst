==================
Testing a Frontier
==================

Frontier Tester is a helper class for easy frontier testing.

Basically it runs a fake crawl against a Frontier, crawl info is faked using a :doc:`Graph Manager <graph-manager>`
instance.

Creating a Frontier Tester
==========================

FrontierTester needs a :doc:`Graph Manager <graph-manager>` and a :class:`FrontierManager` instances::

    >>> from crawlfrontier import FrontierManager, FrontierTester, graphs
    >>> graph = graphs.Manager('sqlite:///graph.db')  # Crawl fake data loading
    >>> frontier = FrontierManager.from_settings()  # Create frontier from default settings
    >>> tester = FrontierTester(frontier, graph)

Running a Test
==============

The tester is now initialized, to run the test just call the method `run`::

    >>> tester.run()

When run method is called the tester will:

    1. Add all the seeds from the graph.
    2. Ask the frontier about next pages.
    3. Fake page response and inform the frontier about page crawl and its links.

Steps 1 and 2 are repeated until crawl or frontier ends.

Once the test is finished, the crawling page ``sequence`` is available as a list of frontier :class:`Page` objects::

    >>> tester.sequence
    [{
        "_": "<Page:0x103609750:A1>",
        "created_at": "2014-11-20T13:20:27.205854",
        "depth": 0,
        "domain": {
            "_": "<Domain:0x1034d2a90:A>",
            "fingerprint": "6dcd4ce23d88e2ee9568ba546c007c63d9131c1b",
            "name": "A",
            "netloc": "A",
            "scheme": "-",
            "sld": "-",
            "subdomain": "-",
            "tld": "-"
        },
        "fingerprint": "1ffd4ba3eb9ffadf4db3c3ff4c1bbcf94a64cc59",
        "last_update": "2014-11-20T13:20:27.271851",
        "meta": {},
        "n_adds": 1,
        "n_crawls": 1,
        "n_errors": 0,
        "n_queued": 1,
        "state": "C",
        "status": "200",
        "url": "A1"
    },
    ...

Test Parameters
===============

In some test cases you may want to add all graph pages as seeds, this can be done with the parameter ``add_all_pages``::

    >>> tester.run(add_all_pages=True)

Maximum number of returned pages per ``get_next_pages`` call can be set using frontier settings, but also can be modified
when creating the FrontierTester with the ``max_next_pages`` argument::

    >>> tester = FrontierTester(frontier, graph, max_next_pages=10)


An example of use
=================

A working example using test data from graphs and :ref:`basic backends <frontier-backends-basic-algorithms>`::

    from crawlfrontier import FrontierManager, Settings, FrontierTester, graphs


    def test_backend(backend):
        # Graph
        graph = graphs.Manager()
        graph.add_site_list(graphs.data.SITE_LIST_02)

        # Frontier
        settings = Settings()
        settings.BACKEND = backend
        settings.TEST_MODE = True
        frontier = FrontierManager.from_settings(settings)

        # Tester
        tester = FrontierTester(frontier, graph)
        tester.run(add_all_pages=True)

        # Show crawling sequence
        print '-'*40
        print frontier.backend.name
        print '-'*40
        for page in tester.sequence:
            print page.url

    if __name__ == '__main__':
        test_backend('crawlfrontier.contrib.backends.memory.heapq.FIFO')
        test_backend('crawlfrontier.contrib.backends.memory.heapq.LIFO')
        test_backend('crawlfrontier.contrib.backends.memory.heapq.BFS')
        test_backend('crawlfrontier.contrib.backends.memory.heapq.DFS')
