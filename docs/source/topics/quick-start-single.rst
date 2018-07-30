==========================
Quick start single process
==========================

The idea is that you develop and debug crawling strategy in single process mode locally and use distributed one when
deploying crawling strategy for crawling in production at scale. Single process is also good as a first step to get
something running quickly.

    Note, that this tutorial doesn't work for :class:`frontera.contrib.backends.memory.MemoryDistributedBackend`.

1. Create your Scrapy spider
============================

Create your Scrapy project as you usually do. Enter a directory where you’d like to store your code and then run::

    scrapy startproject tutorial

This will create a tutorial directory with the following contents::

    tutorial/
        scrapy.cfg
        tutorial/
            __init__.py
            items.py
            pipelines.py
            settings.py
            spiders/
                __init__.py
                ...

These are basically:

- **scrapy.cfg**: the project configuration file
- **tutorial/**: the project’s python module, you’ll later import your code from here.
- **tutorial/items.py**: the project’s items file.
- **tutorial/pipelines.py**: the project’s pipelines file.
- **tutorial/settings.py**: the project’s settings file.
- **tutorial/spiders/**: a directory where you’ll later put your spiders.

2. Install Frontera
===================

See :doc:`installation`.

3. Integrate your spider with the Frontera
==========================================

This article about :doc:`integration with Scrapy <scrapy-integration>` explains this step in detail.

4. Choose your crawling strategy
================================

Here are the options you would need to redefine when running in single process mode the crawler configured for
distributed mode::

    # these two parameters are pointing Frontera that it will run locally

    SPIDER_FEED_PARTITIONS = 1
    SPIDER_LOG_PARTITIONS = 1


5. Choose your backend
======================

Configure frontier settings to use a built-in backend like::

    BACKEND = 'frontera.contrib.backends.sqlalchemy.Distributed'


6. Inject the seed URLs
=======================

This step is required only if your crawling strategy requires seeds injection from external source.::

    $ python -m frontera.utils.add_seeds --config [your_frontera_config] --seeds-file [path to your seeds file]

After script is finished succesfully your seeds should be stored in backend's queue and scheduled for crawling.

7. Run the spider
=================

Run your Scrapy spider as usual from the command line::

    scrapy crawl myspider

And that's it! You got your crawler running integrated with Frontera.

What else?
==========

You’ve seen a simple example of how to use Frontera with Scrapy, but this is just the surface.
Frontera provides many powerful features for making frontier management easy and efficient, such as:

* Built-in support for :ref:`database storage <frontier-backends-sqlalchemy>` for crawled pages.

* Easy :doc:`built-in integration with Scrapy <scrapy-integration>` and :doc:`any other crawler <frontier-api>`
  through its API.

* :ref:`Two distributed crawling modes <use-cases>` with use of ZeroMQ or Kafka and distributed backends.

* Creating different crawling logic/policies :doc:`defining your own backend <frontier-backends>`.

* Plugging your own request/response altering logic using :doc:`middlewares <frontier-middlewares>`.

* Create fake sitemaps and reproduce crawling without crawler with the :doc:`Graph Manager <graph-manager>`.

* :doc:`Record your Scrapy crawls <scrapy-recorder>` and use it later for frontier testing.

* Logging facility that you can hook on to for catching errors and debug your frontiers.

