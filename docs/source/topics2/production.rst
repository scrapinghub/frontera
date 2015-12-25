=====================
Production deployment
=====================

These are the topics you need to consider when deploying Frontera-based crawler in production system.

DNS Service
===========

Along with what was mentioned in :ref:`basic_requirements` you may need also a dedicated DNS Service with caching.
Especially, if your crawler is expected to generate substantial number of DNS queries. It is true for breadth-first
crawling, or any other strategies, implying accessing large number of websites, within short period of time.

Because of huge load DNS service may get blocked by your network provider eventually. We recommend to setup a dedicated
DNS instance locally on every spider machine with upstream using massive DNS caches like OpenDNS or Verizon.


Choose message bus
==================

There are two options available:

* Kafka, requires properly configured partitions,
* ZeroMQ (default), requires broker process.

Configuring Kafka
-----------------
The main thing to do here is to set the number of partitions for :setting:`OUTGOING_TOPIC` equal to the number of spider
instances and for :setting:`INCOMING_TOPIC` equal to number of strategy worker instances. For other topics it makes sense to
set more than one partition to better distribute the load across Kafka cluster.

Kafka throughput is key performance issue, make sure that Kafka brokers have enough IOPS, and monitor the network load.


Configuring ZeroMQ
------------------
ZeroMQ requires almost no configuration except hostname and base port where to bind it's sockets. Please see
:setting:`ZMQ_HOSTNAME` and :setting:`ZMQ_BASE_PORT` settings for more detail. ZeroMQ also requires distributed frontera
broker process running and accessible to connect. See :ref:`running_zeromq_broker`.


Configure Frontera workers
==========================
There are two type of workers: DB and Strategy.

:term:`DB worker` is doing three tasks in particular:

* Reading :term:`spider log` stream and updates metadata in DB,
* Consult lags in message bus, gets new batches and pushes them to :term:`spider feed`,
* Reads :term:`scoring log` stream and updates DB with new score and schedule URLs to download if needed.

Strategy worker is reading :term:`spider log`, calculating score, deciding if URL needs to be crawled and pushes
update_score events to :term:`scoring log`.

Before setting it up you have to decide how many spider instances you need. One spider is able to download and parse
about 700 pages/minute in average. Therefore if you want to fetch 1K per second you probably need about 10 spiders. For
each 4 spiders you would need one pair of workers (strategy and DB). If your strategy worker is lightweight (not
processing content for example) then 1 strategy worker per 15 spider instances could be enough.

Your spider log stream should have as much partitions as *strategy workers* you need. Each
strategy worker is assigned to specific partition using option :setting:`SCORING_PARTITION_ID`.

Your spider feed stream, containing new batches should have as much partitions as *spiders* you will have in your
cluster.

Now, let's create a Frontera workers settings file under ``frontera`` subfolder and name it ``worker_settings.py``. ::

    from distributed_frontera.settings.default_settings import MIDDLEWARES

    MAX_NEXT_REQUESTS = 128     # Size of batch to generate per partition, should be consistent with
                                # CONCURRENT_REQUESTS in spider. General recommendation is 5-7x CONCURRENT_REQUESTS

    #--------------------------------------------------------
    # Url storage
    #--------------------------------------------------------
    BACKEND = 'distributed_frontera.contrib.backends.hbase.HBaseBackend'
    HBASE_DROP_ALL_TABLES = False
    HBASE_THRIFT_PORT = 9090
    HBASE_THRIFT_HOST = 'localhost'

    MIDDLEWARES.extend([
        'frontera.contrib.middlewares.domain.DomainMiddleware',
        'frontera.contrib.middlewares.fingerprint.DomainFingerprintMiddleware'
    ])

    #--------------------------------------------------------
    # Logging
    #--------------------------------------------------------
    LOGGING_EVENTS_ENABLED = False
    LOGGING_MANAGER_ENABLED = True
    LOGGING_BACKEND_ENABLED = True
    LOGGING_DEBUGGING_ENABLED = False


You should add there settings related to message bus you have chosen. Default is ZeroMQ, running on local host.

Configure Frontera spiders
==========================
Next step is to create own Frontera settings file for every spider instance. Often it's a good idea to name
settings file according to partition ids assigned. E.g. ``settingsN.py``. ::

    from distributed_frontera.settings.default_settings import MIDDLEWARES

    MAX_NEXT_REQUESTS = 256     # Should be consistent with MAX_NEXT_REQUESTS set for Frontera worker

    MIDDLEWARES.extend([
        'frontera.contrib.middlewares.domain.DomainMiddleware',
        'frontera.contrib.middlewares.fingerprint.DomainFingerprintMiddleware'
    ])

    #--------------------------------------------------------
    # Crawl frontier backend
    #--------------------------------------------------------
    BACKEND = 'distributed_frontera.backends.remote.KafkaOverusedBackend'
    SPIDER_PARTITION_ID = 0                 # Partition ID assigned

    #--------------------------------------------------------
    # Logging
    #--------------------------------------------------------
    LOGGING_ENABLED = True
    LOGGING_EVENTS_ENABLED = False
    LOGGING_MANAGER_ENABLED = False
    LOGGING_BACKEND_ENABLED = False
    LOGGING_DEBUGGING_ENABLED = False

Again, add message bus related options.

You should end up having as much settings files as spider instances your system will have. You can also store permanent
options in common module, and import it's contents from each instance-specific config file.

It is recommended to run spiders on a dedicated machines, they quite likely to consume lots of CPU and network
bandwidth.

The same thing have to be done for strategy workers, each strategy worker should have it's own partition id
(see :setting:`SCORING_PARTITION_ID`) assigned in config files named ``strategyN.py``.


Starting the cluster
====================

First, let's start storage worker. It's recommended to dedicate one worker instance for new batches generation and
others for the rest. Batch generation instance isn't much dependent on the count of spider instances, but saving
to storage is. Here is how to run all in the same process::

    # start DB worker, enabling batch generation, DB saving and scoring log consumption
    $ python -m distributed_frontera.worker.main --config frontera.worker_settings


Next, let's start strategy worker with sample strategy for crawling the internet in Breadth-first manner.::

    $ python -m distributed_frontera.worker.score --config frontera.strategy0 --strategy distributed_frontera.worker.strategy.bfs
    $ python -m distributed_frontera.worker.score --config frontera.strategy1 --strategy distributed_frontera.worker.strategy.bfs
    ...
    $ python -m distributed_frontera.worker.score --config frontera.strategyN --strategy distributed_frontera.worker.strategy.bfs

You should notice that all processes are writing messages to the output. It's ok if nothing is written in streams,
because of absence of seed URLs in the system.

Let's put our seeds in text file, one URL per line.
Starting the spiders:::

    $ scrapy crawl tutorial -L INFO -s FRONTERA_SETTINGS=frontera.settings0 -s SEEDS_SOURCE = 'seeds.txt'
    ...
    $ scrapy crawl tutorial -L INFO -s FRONTERA_SETTINGS=frontera.settings1
    $ scrapy crawl tutorial -L INFO -s FRONTERA_SETTINGS=frontera.settings2
    $ scrapy crawl tutorial -L INFO -s FRONTERA_SETTINGS=frontera.settings3
    ...
    $ scrapy crawl tutorial -L INFO -s FRONTERA_SETTINGS=frontera.settingsN

You should end up with N spider processes running. Each should read it's own Frontera config, and first one is using
``SEEDS_SOURCE`` variable to pass seeds to Frontera cluster.

After some time seeds will pass the streams and get scheduled for downloading by workers. Crawler is bootstrapped.

