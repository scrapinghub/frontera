==========
Quickstart
==========

Here is a guide how to quickly setup Frontera for single-machine, multiple process, local hacking. We're going to deploy
the simpliest possible setup with HBase and ZeroMQ. Please proceed to :doc:`production` article for a production setup
details.

.. _basic_requirements:

Prerequisites
=============

Here is what services needs to be installed and configured before running Frontera:

- HBase

These can be set up locally, no specific tuning is needed.
Also you need to have installed Python 2.7+ and Scrapy library.

Frontera installation
---------------------
For Ubuntu, type in command line: ::

    $ pip install distributed-frontera

Create a namespace in the HBase shell for the crawler: ::

    $ bin/hbase shell
    > create_namespace 'crawler'


Checkout a simple Scrapy spider
===============================
This is a general spider, it does almost nothing except extracting links from downloaded content. Also contains some
of predefined options, please consult settings reference to get more information. ::

    $ git clone https://github.com/sibiryakov/general-spider.git


.. _running_zeromq_broker:

Start cluster
=============

First, make sure that the HBase Thrift server is up and running. Next, let's start ZeroMQ broker. ::

    $ python -m distributed_frontera.messagebus.zeromq.broker

You should see a log output of broker with statistics on messages transmitted.

Second, let's start DB worker. ::

    $ python -m distributed_frontera.worker.main --config frontier.workersettings


Next, let's start strategy worker with default BFS (Breadth-First Strategy)::

    $ python -m distributed_frontera.worker.score --config frontier.strategy0 --strategy distributed_frontera.worker.strategy.bfs


You should notice that all processes are writing messages to the output. It's ok if nothing is written in ZeroMQ
sockets, because of absence of seed URLs in the system.

There are Spanish (.es zone) internet URLs from DMOZ directory in general spider repository, let's use them as seeds to bootstrap
crawling.
Starting the spiders: ::

    $ scrapy crawl general -L INFO -s FRONTERA_SETTINGS=frontier.spider0 -s SEEDS_SOURCE=seeds_es_dmoz.txt
    $ scrapy crawl general -L INFO -s FRONTERA_SETTINGS=frontier.spider1


You should end up with 2 spider processes running. Each should read it's own Frontera config, and first one is using
``SEEDS_SOURCE`` option to read seeds to bootstrap Frontera cluster.

After some time seeds will pass the streams and will be scheduled for downloading by workers. At this moment crawler
is bootstrapped. Now you can periodically check DB worker output or ``metadata`` table contents to see that there is
actual activity.
