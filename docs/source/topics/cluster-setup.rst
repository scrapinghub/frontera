===================
Cluster setup guide
===================

This guide is targeting an initial setup of crawling cluster, probably further tuning will be needed. This guide implies
you use Kafka message bus for cluster setup (recommended), although it is also possible to use ZeroMQ, which is less
reliable option.

Things to decide
================
* The speed you want to crawl with,
* number of spider processes (assuming that single spider process gives a maximum of 1200 pages/min),
* number of DB and Strategy worker processes.

Things to setup before you start
================================
* Kafka,
* HBase (we recommend 1.0.x and higher),
* :doc:`DNS Service <dns-service>` (recommended but not required).

Things to implement before you start
====================================
* :doc:`Crawling strategy <own_crawling_strategy>`
* Spider code

Configuring Kafka
=================
Create all topics needed for Kafka message bus
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* :term:`spider log` (`frontier-done` (see :setting:`SPIDER_LOG_TOPIC`)), set the number of partitions equal to number of
  strategy worker instances,
* :term:`spider feed` (`frontier-todo` (see :setting:`SPIDER_FEED_TOPIC`)), set the number of partitions equal to number of
  spider instances,
* :term:`scoring log` (`frontier-score` (see :setting:`SCORING_LOG_TOPIC`))


Configuring HBase
=================
* create a namespace ``crawler`` (see :setting:`HBASE_NAMESPACE`),
* make sure Snappy compression is supported natively.


Configuring Frontera
====================
Every Frontera component requires it's own configuration module, but some options are shared, so we recommend to create
a common modules and import settings from it in component's modules.

1. Create a common module and add there: ::

    from __future__ import absolute_import
    from frontera.settings.default_settings import MIDDLEWARES
    MAX_NEXT_REQUESTS = 512
    SPIDER_FEED_PARTITIONS = 2 # number of spider processes
    SPIDER_LOG_PARTITIONS = 2 # worker instances
    MIDDLEWARES.extend([
        'frontera.contrib.middlewares.domain.DomainMiddleware',
        'frontera.contrib.middlewares.fingerprint.DomainFingerprintMiddleware'
    ])

    QUEUE_HOSTNAME_PARTITIONING = True
    KAFKA_LOCATION = 'localhost:9092' # your Kafka broker host:port
    SCORING_TOPIC = 'frontier-scoring'
    URL_FINGERPRINT_FUNCTION='frontera.utils.fingerprint.hostname_local_fingerprint'

2. Create workers shared module: ::

    from __future__ import absolute_import
    from .common import *

    BACKEND = 'frontera.contrib.backends.hbase.HBaseBackend'

    MAX_NEXT_REQUESTS = 2048
    NEW_BATCH_DELAY = 3.0

    HBASE_THRIFT_HOST = 'localhost' # HBase Thrift server host and port
    HBASE_THRIFT_PORT = 9090

3. Create DB worker module: ::

    from __future__ import absolute_import
    from .worker import *

    LOGGING_CONFIG='logging-db.conf' # if needed

4. Create Strategy worker's module: ::

    from __future__ import absolute_import
    from .worker import *

    CRAWLING_STRATEGY = '' # path to the crawling strategy class
    LOGGING_CONFIG='logging-sw.conf' # if needed

The logging can be configured according to https://docs.python.org/2/library/logging.config.html see the
:doc:`list of loggers <loggers>`.

5. Configure spiders module: ::

    from __future__ import absolute_import
    from .common import *

    BACKEND = 'frontera.contrib.backends.remote.messagebus.MessageBusBackend'
    KAFKA_GET_TIMEOUT = 0.5


6. Configure Scrapy settings module. It's located in Scrapy project folder and referenced in scrapy.cfg. Let's add
there::

    FRONTERA_SETTINGS = ''  # module path to your Frontera spider config module

    SCHEDULER = 'frontera.contrib.scrapy.schedulers.frontier.FronteraScheduler'

    SPIDER_MIDDLEWARES = {
        'frontera.contrib.scrapy.middlewares.schedulers.SchedulerSpiderMiddleware': 999,
        'frontera.contrib.scrapy.middlewares.seeds.file.FileSeedLoader': 1,
    }
    DOWNLOADER_MIDDLEWARES = {
        'frontera.contrib.scrapy.middlewares.schedulers.SchedulerDownloaderMiddleware': 999,
    }


Starting the cluster
====================

First, let's start storage worker: ::

    # start DB worker only for batch generation
    $ python -m frontera.worker.db --config [db worker config module] --no-incoming
    ...
    # Then start next one dedicated to spider log processing
    $ python -m frontera.worker.db --no-batches --config [db worker config module]


Next, let's start strategy workers, one process per spider log partition: ::

    $ python -m frontera.worker.strategy --config [strategy worker config] --partition-id 0
    $ python -m frontera.worker.strategy --config [strategy worker config] --partition-id 1
    ...
    $ python -m frontera.worker.strategy --config [strategy worker config] --partition-id N

You should notice that all processes are writing messages to the log. It's ok if nothing is written in streams,
because of absence of seed URLs in the system.

Let's put our seeds in text file, one URL per line and start spiders. A single spider per spider feed partition: ::

    $ scrapy crawl [spider] -L INFO -s SEEDS_SOURCE = 'seeds.txt' -s SPIDER_PARTITION_ID=0
    ...
    $ scrapy crawl [spider] -L INFO -s SPIDER_PARTITION_ID=1
    $ scrapy crawl [spider] -L INFO -s SPIDER_PARTITION_ID=2
    ...
    $ scrapy crawl [spider] -L INFO -s SPIDER_PARTITION_ID=N

You should end up with N spider processes running. Usually it's enough for a single instance to read seeds from
``SEEDS_SOURCE`` variable to pass seeds to Frontera cluster. Seeds are only read if spider queue is empty.
::setting:`SPIDER_PARTITION_ID` can be read from config file also.

After some time seeds will pass the streams and will be scheduled for downloading by workers. Crawler is bootstrapped.
