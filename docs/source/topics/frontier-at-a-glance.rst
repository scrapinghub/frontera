==========================
Frontera at a glance
==========================

Frontera is an application framework that is meant to be used as part of a `Crawling System`_, allowing you to
easily manage and define tasks related to a :doc:`crawl frontier <what-is-frontera>`.

Even though it was originally designed for `Scrapy`_, it can also be used with any other Crawling Framework/System as
the framework offers a generic frontier functionality.

The purpose of this document is to introduce you to the concepts behind Frontera so that you can get an idea of
how it works and to decide if it is suited to your needs.


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

This article about :doc:`integration with Scrapy <scrapy-integration>` explains this step in detail.

3. Choose your backend
======================

Configure frontier settings to use a built-in backend like in-memory BFS::

    BACKEND = 'frontera.contrib.backends.memory.heapq.BFS'

4. Run the spider
=================

Run your Scrapy spider as usual from the command line::

    scrapy crawl myspider

And that's it! You got your spider running integrated with Frontera.

What else?
==========

You’ve seen a simple example of how to use Frontera with Scrapy, but this is just the surface.
Frontera provides many powerful features for making Frontier management easy and efficient, such as:

* Easy :doc:`built-in integration with Scrapy <scrapy-integration>` and :doc:`any other crawler <frontier-api>`
  through its API.

* Creating different crawling logic/policies :doc:`defining your own backend <frontier-backends>`.

* Built-in support for :ref:`database storage <frontier-backends-sqlalchemy>` for crawled pages.

* Support for extending Frontera by plugging your own functionality using :doc:`middlewares <frontier-middlewares>`.

* Built-in middlewares for:

  * Extracting :ref:`domain info <frontier-domain-middleware>` from page URLs.
  * Create :ref:`unique fingerprints for page URLs <frontier-url-fingerprint-middleware>` and :ref:`domain names <frontier-domain-fingerprint-middleware>`.

* Create fake sitemaps and reproduce crawling without crawler with the :doc:`graph Manager <graph-manager>`.

* Tools for :doc:`easy frontier testing <frontier-tester>`.

* :doc:`Record your Scrapy crawls <scrapy-recorder>` and use it later for frontier testing.

* Logging facility that you can hook on to for catching errors and debug your frontiers.


What's next?
============

The next obvious steps are for you to :doc:`install Frontera <installation>`, and read the
:doc:`architecture overview <architecture>` and :doc:`API docs <frontier-api>`. Thanks for your interest!



.. _Crawling System: http://en.wikipedia.org/wiki/Web_crawler
.. _Scrapy: http://scrapy.org/
