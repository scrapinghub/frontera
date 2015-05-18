=============================
Distributed Frontera tutorial
=============================

Frontera can be set up in standalone and distributed manner. Allowing to distribute spiders, backend communication, and
strategy workers. All the communication between components is done using `Kafka messaging system`_. Though,
theoretically it should be possible to use other transport than Kafka, practically nothing is done in that direction.
This tutorial is mainly written for Scrapy-based spiders, but nothing prevents to use any other fetching component not
necessary written in Python.

1. Prerequisites
================

Here is what services needs to be installed and configured in order to run a distributed Frontera cluster:

* Kafka,

* HBase,

* Scrapy,

* DNS Service using some big DNS like OpenDNS or Verizon as upstream (if you're going to run a broad crawls).


It is recommended to run spiders on a dedicated machines, they quite likely to consume lots of CPU and network
bandwidth.

2. Create a simple Scrapy spider
================================
Creation of basic Scrapy spider is described :doc:`here <frontier-at-a-glance>`. However, for broad crawling please
consider following:

Adding one of seed loaders for bootstrapping of crawling process::

    SPIDER_MIDDLEWARES.update({
        'crawlfrontier.contrib.scrapy.middlewares.seeds.file.FileSeedLoader': 1,
    })


Various settings suitable for broad crawling::

    HTTPCACHE_ENABLED = False   # Turns off disk cache, which has low hit ratio during broad crawls
    REDIRECT_ENABLED = True
    COOKIES_ENABLED = False
    DOWNLOAD_TIMEOUT = 120
    RETRY_ENABLED = False   # Retries can be handled by Frontera itself, depending on crawling strategy
    DOWNLOAD_MAXSIZE = 10 * 1024 * 1024  # Maximum document size, causes OOM kills
    LOGSTATS_INTERVAL = 10  # Print stats every 10 secs to console

Auto throttling and concurrency settings for polite and responsible crawling:::

    # auto throttling
    AUTOTHROTTLE_ENABLED = True
    AUTOTHROTTLE_DEBUG = False
    AUTOTHROTTLE_MAX_DELAY = 3.0
    AUTOTHROTTLE_START_DELAY = 0.25     # Any small enough value, it will be adjusted during operation by averaging
                                        # with response latencies.
    RANDOMIZE_DOWNLOAD_DELAY = False

    # concurrency
    CONCURRENT_REQUESTS = 256           # Depends on many factors, and should be determined experimentally
    CONCURRENT_REQUESTS_PER_DOMAIN = 10
    DOWNLOAD_DELAY = 0.0

Check also `Scrapy broad crawling`_ recommendations.

It's also a good practice to prevent spider from closing because of insufficiency of queued requests transport:::

    # Scrapy stable branch
    def set_crawler(self, crawler):
        super(TutorialSpider, self).set_crawler(crawler)
        self.crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)

    # master latest changes
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(*args, **kwargs)
        spider._set_crawler(crawler)
        spider.crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider


3. Implement the crawling strategy
==================================
Use ``crawlfrontier.worker.strategy.spanish`` module for reference. In general, you need to write a ``CrawlingStrategy``
 class with above interface::

    class CrawlStrategy(object):
        def __init__(self):
            pass

        def add_seeds(self, seeds):
            pass

        def page_crawled(self, response, links):
            pass

        def page_error(self, request, error):
            pass

        def finished(self):
            return False

        def get_score(self, url):
            return 1.0

All the incoming results from spiders will be passed thru this interface and for each URL the score should be
calculated and returned by method ``get_score``. Periodically ``finished()`` method is called to check if crawling goal
is achieved. The strategy class instantiated in strategy worker, and can use it's own storage or any other kind of
resources.

4. Configure Frontera workers
=============================
There are two type of workers: Storage and Strategy.

Storage worker is responsible for communicating with storage DB, and mainly saving metadata and content along with
retrieving new batches to download.

Three tasks it is doing in particular:

* Reading ``INCOMING_TOPIC`` and update metadata in DB,

* Consult lags in Kafka, gets new batches and pushes them to ``OUTGOING_TOPIC``,

* Read ``SCORING_TOPIC`` update DB with new score and schedule URL to download if needed.

Strategy worker is reading ``INCOMING_TOPIC``, calculating score, deciding if URL needs to be crawled and pushes
update_score events to ``SCORING_TOPIC``.

Before setting it up you have to decide how many spider instances you need. One spider is able to download and parse
about 700 pages/minute in average. Therefore if you want to fetch 1K per second you probably need about 10 spiders. For
each 4 spiders you would need one storage worker. If your strategy worker is lightweight (not processing content for
example) then 1 strategy worker per 15 spider instances could be enough.

Now, let's create a Frontera workers settings file under ``frontera`` subfolder and name it ``worker_settings.py``.::

    from crawlfrontier.settings.default_settings import MIDDLEWARES

    MAX_REQUESTS = 0
    MAX_NEXT_REQUESTS = 128     # Size of batch to generate per partition, should be consistent with
                                # CONCURRENT_REQUESTS in spider. General recommendation is 5-7x CONCURRENT_REQUESTS
    CONSUMER_BATCH_SIZE = 512   # Batch size for updates to backend storage
    NEW_BATCH_DELAY = 30.0      # This cause spider to wait for specified time, after getting empty response from
                                # backend

    #--------------------------------------------------------
    # Url storage
    #--------------------------------------------------------
    BACKEND = 'crawlfrontier.contrib.backends.hbase.HBaseBackend'
    HBASE_DROP_ALL_TABLES = False
    HBASE_THRIFT_PORT = 9090
    HBASE_THRIFT_HOST = 'localhost'
    HBASE_QUEUE_PARTITIONS = 2  # Count of spider instances

    MIDDLEWARES.extend([
        'crawlfrontier.contrib.middlewares.domain.DomainMiddleware',
        'crawlfrontier.contrib.middlewares.fingerprint.DomainFingerprintMiddleware'
    ])

    KAFKA_LOCATION = 'localhost:9092'
    FRONTIER_GROUP = 'scrapy-crawler'
    INCOMING_TOPIC = 'frontier-done'    # Topic used by spiders where to send fetching results
    OUTGOING_TOPIC = 'frontier-todo'    # Requests that needs to be downloaded is written there
    SCORING_GROUP = 'scrapy-scoring'
    SCORING_TOPIC = 'frontier-score'    # Scores provided by strategy worker using this channel and read by storage
                                        # worker.

    #--------------------------------------------------------
    # Logging
    #--------------------------------------------------------
    LOGGING_EVENTS_ENABLED = False
    LOGGING_MANAGER_ENABLED = True
    LOGGING_BACKEND_ENABLED = True
    LOGGING_DEBUGGING_ENABLED = False


5. Configure Frontera spiders
=============================
Next step is to create own file Frontera settings file for every spider instance. It is recommended to name settings
file according to partition ids assigned. E.g. ``settingsN.py``. ::

    from crawlfrontier.settings.default_settings import MIDDLEWARES

    MAX_REQUESTS = 0
    MAX_NEXT_REQUESTS = 256     # Should be consistent with MAX_NEXT_REQUESTS set for Frontera worker

    MIDDLEWARES.extend([
        'crawlfrontier.contrib.middlewares.domain.DomainMiddleware',
        'crawlfrontier.contrib.middlewares.fingerprint.DomainFingerprintMiddleware'
    ])

    #--------------------------------------------------------
    # Crawl frontier backend
    #--------------------------------------------------------
    BACKEND = 'crawlfrontier.contrib.backends.remote.KafkaOverusedBackend'
    KAFKA_SERVER = 'dist-cf1.scrapinghub.com:9092'
    KAFKA_PARTITION_ID = 0      # Partition ID assigned

    #--------------------------------------------------------
    # Logging
    #--------------------------------------------------------
    LOGGING_ENABLED = True
    LOGGING_EVENTS_ENABLED = False
    LOGGING_MANAGER_ENABLED = False
    LOGGING_BACKEND_ENABLED = False
    LOGGING_DEBUGGING_ENABLED = False

You should end up having as much settings files as your system spider instances will have.

6. Create Kafka topics
======================
The main thing to do here is to set the number of partitions for ``OUTGOING_TOPIC`` equal to the number of spider
instances. For other topics it makes sense to set more than one partition to better distribute the load across Kafka
cluster.

7. Start cluster
================

First, let's start storage worker. It's recommended to dedicate one worker instance for new batches generation and
others for the rest. Batch generation instance isn't much dependent on the count of spider isntances, but saving
to storage is.::

    # start the batch generation instance
    $ python -m crawlfrontier.worker.main --config frontier.worker_settings --no-incoming --no-scoring

    # start saving instances
    $ python -m crawlfrontier.worker.main --no-batches --config frontier.worker_settings --port 6012


Next, let's start strategy worker with sample strategy for crawling the Spanish internet. You can use yours, if you
have it.::

    $ python -m crawlfrontier.worker.score --config frontier.worker_settings --strategy
        crawlfrontier.worker.strategy.spanish

You should notice that all processes are writing messages to the logs. It's ok if nothing is written in Kafka topics,
because of absence of seed URLs in the system.

Let's put our seeds in text file, one URL per line.
Starting the spiders:::

    $ scrapy crawl tutorial -L INFO -s FRONTIER_SETTINGS=frontier.settings0 -s SEEDS_SOURCE = 'seeds.txt'
    ...
    $ scrapy crawl tutorial -L INFO -s FRONTIER_SETTINGS=frontier.settings1
    $ scrapy crawl tutorial -L INFO -s FRONTIER_SETTINGS=frontier.settings2
    $ scrapy crawl tutorial -L INFO -s FRONTIER_SETTINGS=frontier.settings3
    ...
    $ scrapy crawl tutorial -L INFO -s FRONTIER_SETTINGS=frontier.settingsN

You should end up with N spider processes running. Each should read it's own Frontera config, and first one is using
SEEDS_SOURCE variable to pass seeds to Frontera cluster.

After some time seeds will pass the Kafka topics and get scheduled for downloading by workers. Crawler is bootstrapped.

.. _`Kafka messaging system`: http://kafka.apache.org/
.. _`Scrapy broad crawling`: http://doc.scrapy.org/en/master/topics/broad-crawls.html

