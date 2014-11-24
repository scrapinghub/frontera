==========================
Crawl Frontier at a glance
==========================

Crawl Frontier is an application framework that is meant to be used as part of a `Crawling System`_, allowing to easily
manage and define tasks related to a :doc:`Crawling Frontier <what-is-a-crawl-frontier>`.

Even though it was originally designed for `Scrapy`_, it can also be used with any other Crawling Framework/System as
the framework offers a generic frontier functionallity.

The purpose of this document is to introduce you to the concepts behind Crawl Frontier so you can get an idea of how it
works and decide if it fits your needs.


1. Create your crawler
======================

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


2. Integrate your crawler with the frontier
===========================================

Add the Scrapy Crawl Frontier middlewares to your settings::

    SPIDER_MIDDLEWARES.update({
        'crawlfrontier.contrib.scrapy.middlewares.frontier.CrawlFrontierSpiderMiddleware': 1000,
    })

    DOWNLOADER_MIDDLEWARES.update({
        'crawlfrontier.contrib.scrapy.middlewares.frontier.CrawlFrontierDownloaderMiddleware': 1000,
    })

Create a Crawl Frontier settings.py file and add it to your Scrapy settings::

    FRONTIER_SETTINGS = 'tutorial/frontier/settings.py'

3. Choose your backend
======================

Configure frontier settings to use a built-in backend like in-memory BFS::

    BACKEND = 'crawlfrontier.contrib.backends.memory.heapq.BFS'

4. Run the spider
=================

Run your Scrapy spider as usual from the command line::

    scrapy crawl myspider

And that's it! You got your spider running integrated with Crawl Frontier.

What else?
==========

You’ve seen a simple example of how to use Crawl Frontier with Scrapy, but this is just the surface.
Crawl provides a lot of powerful features for making Frontier managemen easy and efficient, such as:

* Easy :doc:`built-in integration with Scrapy <scrapy-integration>` and :doc:`any other crawler <frontier-api>`
  through its API.

* Creating different crawling logic/policies :doc:`defining your own backend <frontier-backends>`.

* Built-in support for :ref:`database storage <frontier-backends-sqlalchemy>` for crawled pages.

* Support for extending Crawl Frontier by plugging your own functionality using :doc:`middlewares <frontier-middlewares>`.

* Built-in middlewares for:

  * Extracting :ref:`domain info <frontier-domain-middleware>` from page URLs.
  * Create :ref:`unique fingerprints for page URLs <frontier-url-fingerprint-middleware>` and :ref:`domain names <frontier-domain-fingerprint-middleware>`.

* Create fake sitemaps and reproduce crawling without crawler with the :doc:`graph Manager <graph-manager>`.

* Tools for :doc:`easy frontier testing <frontier-tester>`.

* :doc:`Record your Scrapy crawls <scrapy-recorder>` and use it later for frontier testing.

* Logging facility that you can hook on to for catching errors and debug your frontiers.


What's next?
============

The next obvious steps are for you to :doc:`install Crawl Frontier <installation>`, and read the
:doc:`architecture overview <architecture>` and :doc:`API docs <frontier-api>`. Thanks for your interest!



.. _Crawling System: http://en.wikipedia.org/wiki/Web_crawler
.. _Scrapy: http://scrapy.org/
