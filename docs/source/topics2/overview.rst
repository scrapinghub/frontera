========
Overview
========

There are two related projects `Frontera`_ and Distributed Frontera. In this documentation by Frontera we will be
referring to distributed version if explicitly not mentioned opposite.

Use cases
---------


Architecture
------------
Overall system forms a closed circle and all the components are working as daemons in infinite cycles.
`Kafka`_ or `ZeroMQ`_ can be used as a data bus, storage is `HBase`_ and fetching is done using `Scrapy`_. There is a
transport and storage layer abstractions, so one can plug it's own data bus or transport. There are instances of three
types:

- **Spiders** or fetchers, implemented using Scrapy (sharded).
    Responsible for resolving DNS queries, getting content from the Internet and doing link (or other data) extraction
    from content.
- **Strategy workers** (sharded).
    Run the crawling strategy code: scoring the links, deciding if link needs to be scheduled and when to stop crawling.
- **DB workers** (replicated).
    Store all the metadata, including scores and content, and generating new batches for downloading by spiders.

Where *sharded* means component consumes messages of assigned partition only, e.g. processes certain share of the topic,
and *replicated* is when components consume topic regardless of partitioning.

Such design allows to operate in real-time. Crawling strategy can be changed without having to stop the crawl. Also
:doc:`crawling strategy <customization/own_crawling_strategy>` can be implemented as a separate module; containing logic
for checking the crawling stopping condition, URL ordering, and scoring model.

Frontera is polite to web hosts by design and each host is downloaded by no more than one spider process.
This is achieved by Kafka topic partitioning.

.. image:: images/frontera-design.png

Data flow
---------
Let’s start with spiders. The seed URLs defined by the user inside spiders are propagated to strategy workers and DB
workers by means of ‘Spider Log’ stream. Strategy workers decide which pages to crawl using HBase’s state
cache, assigns a score to each page and sends the results to the ‘Scoring Log’ stream.

DB Worker stores all kinds of metadata, including content and scores. Also DB worker checks for the spider’s consumers
offsets and generates new batches if needed and sends them to 'Spider Feed' stream. Spiders consume these batches,
downloading each page and extracting links from them. The links are then sent to the ‘Spider Log’ stream where they are
stored and scored. That way the flow repeats indefinitely.

.. _`Kafka`: http://kafka.apache.org/
.. _`ZeroMQ`: http://zeromq.org/
.. _`HBase`: http://hbase.apache.org/
.. _`Scrapy`: http://scrapy.org/
.. _`Frontera`: http://github.com/scrapinghub/frontera