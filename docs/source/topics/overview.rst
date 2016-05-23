====================
Frontera at a glance
====================

Frontera is an implementation of crawl frontier, a web crawler component used for accumulating URLs/links before
downloading them from the web. Main features of Frontera are:

* Online processing oriented,
* distributed spiders and backends architecture,
* customizable crawling policy,
* easy integration with Scrapy,
* relational databases support (MySQL, PostgreSQL, sqlite, and more) with `SQLAlchemy`_ and `HBase`_ key-value database
  out of the box,
* `ZeroMQ`_ and `Kafka`_ message bus implementations for distributed crawlers,
* precise crawling logic tuning with crawling emulation using fake sitemaps with the
  :doc:`Graph Manager <graph-manager>`.
* transparent transport layer concept (:term:`message bus`) and communication protocol,
* pure Python implementation.


.. _use-cases:

Use cases
---------

Here are few cases, external crawl frontier can be suitable for:

* URL ordering/queueing isolation from the spider (e.g. distributed cluster of spiders, need of remote management of
  ordering/queueing),
* URL (meta)data storage is needed (e.g. to demonstrate it's contents somewhere),
* advanced URL ordering logic is needed, when it's hard to maintain code within spider/fetcher.


One-time crawl, few websites
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For such use case probably single process mode would be the most appropriate. Frontera can offer these prioritization
models out of the box:

* FIFO,
* LIFO,
* Breadth-first (BFS),
* Depth-first (DFS),
* based on provided score, mapped from 0.0 to 1.0.

If website is big, and it's expensive to crawl the whole website, Frontera can be suitable for pointing the crawler to
the most important documents.


Distributed load, few websites
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If website needs to be crawled faster than single spider one could use distributed spiders mode. In this mode Frontera
is distributing spider processes and using one instance of backend worker. Requests are distributed using
:term:`message bus` of your choice and distribution logic can be adjusted using custom partitioning. By default requests
are distributed to spiders randomly, and desired request rate can be set in spiders.

Consider also using proxy services, such as `Crawlera`_.


Revisiting
^^^^^^^^^^

There is a set of websites and one need to re-crawl them on timely (or other) manner. Frontera provides simple
revisiting backend, scheduling already visited documents for next visit using time interval set by option. This
backend is using general relational database for persistence and can be used in single process or distributed
spiders modes.

Watchdog use case - when one needs to be notified about document changes, also could be addressed with such a backend
and minor customization.


Broad crawling
^^^^^^^^^^^^^^

This use case requires full distribution: spiders and backend. In addition to spiders process one should be running
:term:`strategy worker` (s) and :term:`db worker` (s), depending on chosen partitioning scheme.

Frontera can be used for broad set of tasks related to large scale web crawling:

* Broad web crawling, arbitrary number of websites and pages (we tested it on 45M documents volume and 100K websites),
* Host-focused crawls: when you have more than 100 websites,
* Focused crawling:

    * Topical: you search for a pages about some predefined topic,
    * PageRank, HITS or other link graph algorithm guided.

Here are some of the real world problems:

* Building a search engine with content retrieval from the web.
* All kinds of research work on web graph: gathering links, statistics, structure of graph, tracking domain count, etc.
* More general focused crawling tasks: e.g. you search for pages that are big hubs, and frequently changing in time.

.. _`Frontera`: http://github.com/scrapinghub/frontera
.. _`Crawlera`: http://crawlera.com/
.. _`Kafka`: http://kafka.apache.org/
.. _`ZeroMQ`: http://zeromq.org/
.. _`HBase`: http://hbase.apache.org/
.. _`Scrapy`: http://scrapy.org/
.. _`SQLAlchemy`: http://www.sqlalchemy.org/
