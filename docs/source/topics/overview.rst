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
* Python 3 support.


.. _use-cases:

Use cases
---------

Here are few cases, external crawl frontier can be suitable for:

* URL ordering/queueing isolation from the spider (e.g. distributed cluster of spiders, need of remote management of
  ordering/queueing),
* URL (meta)data storage is needed (e.g. to be able to pause and resume the crawl),
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


Broad crawling of many websites
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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
