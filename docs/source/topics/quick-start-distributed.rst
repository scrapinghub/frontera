============================
Quick start distributed mode
============================

Here is a guide how to quickly setup Frontera for single-machine, multiple process, local hacking. We're going to deploy
the simpliest possible setup with SQLite and ZeroMQ. Please proceed to :doc:`cluster-setup` article for a
production setup details.

Our crawler will have absolute minimum of components needed to work 1 :term:`spider`, 1 :term:`strategy worker` and
1 batch-gen, scoring worker.

.. _basic_requirements:

Prerequisites
=============

Here is what needs to be installed and configured before running Frontera:

- Python 2.7+ or 3.4+
- Scrapy

Frontera installation
---------------------
For Ubuntu, type in command line: ::

    $ pip install frontera[distributed,zeromq,sql]


Get a spider example code
=========================

First checkout a GitHub Frontera repository:
::

    $ git clone https://github.com/scrapinghub/frontera.git

There is a general spider example in ``examples/general-spider`` folder.

This is a general spider, it does almost nothing except extracting links from downloaded content. It also contains some
settings files, please consult :doc:`settings reference <frontera-settings>` to get more information.

.. _running_zeromq_broker:

Start cluster
=============

    IMPORTANT! Because we're using ZeroMQ, and queue is stored in memory the order of the components starting is
    important, please follow as described.

First, let's start ZeroMQ broker. ::

    $ python -m frontera.contrib.messagebus.zeromq.broker

You should see a log output of broker with statistics on messages transmitted.

All further commands have to be made from ``general-spider`` root directory.

Second, there are Spanish (.es zone) internet URLs from DMOZ directory in general spider repository, let's use them as
seeds to bootstrap crawling::

    $ python -m frontera.utils.add_seeds --config config.dbw --seeds-file seeds_es_smp.txt

You should notice the log output and message saying that seeds addition is finished.

Third, starting the :term:`strategy worker`::

    $ python -m frontera.worker.strategy --config config.sw

Fourth, starting the Scrapy spider::

    $ python -m scrapy crawl general

Finally, the DB worker::

    $ python -m frontera.worker.db --no-incoming --config config.dbw --partitions 0

You should notice in logs that DB worker is trying to generate batches and after a short period the Scrapy is crawling
pages, also check the stats change in ZMQ broker and strategy worker. That's it, crawler is running with default
:term:`crawling strategy`.
