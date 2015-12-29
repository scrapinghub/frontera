============================
Quick start distributed mode
============================

Here is a guide how to quickly setup Frontera for single-machine, multiple process, local hacking. We're going to deploy
the simpliest possible setup with SQLite and ZeroMQ. Please proceed to :doc:`production-broad-crawling` article for a
production setup details for broad crawlers.

.. _basic_requirements:

Prerequisites
=============

Here is what services needs to be installed and configured before running Frontera:

- Python 2.7+
- Scrapy

Frontera installation
---------------------
For Ubuntu, type in command line: ::

    $ pip install frontera[distributed,zeromq,sql]


Checkout a simple Scrapy spider
===============================
This is a general spider, it does almost nothing except extracting links from downloaded content. It also contains some
of predefined options, please consult settings reference to get more information. ::

    $ git clone https://github.com/sibiryakov/general-spider.git


.. _running_zeromq_broker:

Start cluster
=============

First, let's start ZeroMQ broker. ::

    $ python -m frontera.contrib.messagebus.zeromq.broker

You should see a log output of broker with statistics on messages transmitted.

All further commands have to be made from ``general-spider`` root directory.

Second, let's start DB worker. ::

    $ python -m frontera.worker.main --config frontier.workersettings


You should notice that DB is writing messages to the output. It's ok if nothing is written in ZeroMQ sockets, because
of absence of seed URLs in the system.

There are Spanish (.es zone) internet URLs from DMOZ directory in general spider repository, let's use them as
seeds to bootstrap crawling.
Starting the spiders: ::

    $ scrapy crawl general -L INFO -s FRONTERA_SETTINGS=frontier.spider0 -s SEEDS_SOURCE=seeds_es_dmoz.txt
    $ scrapy crawl general -L INFO -s FRONTERA_SETTINGS=frontier.spider1


You should end up with 2 spider processes running. Each should read it's own Frontera config, and first one is using
``SEEDS_SOURCE`` option to read seeds to bootstrap Frontera cluster.

After some time seeds will pass the streams and will be scheduled for downloading by workers. At this moment crawler
is bootstrapped. Now you can periodically check DB worker output or ``metadata`` table contents to see that there is
actual activity.
