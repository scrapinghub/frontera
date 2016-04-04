==========================
Quick start single process
==========================

1. Create your spider
=====================

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


4. Choose your backend
======================

Configure frontier settings to use a built-in backend like in-memory BFS::

    BACKEND = 'frontera.contrib.backends.memory.BFS'

5. Run the spider
=================

Run your Scrapy spider as usual from the command line::

    scrapy crawl myspider

And that's it! You got your spider running integrated with Frontera.

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





