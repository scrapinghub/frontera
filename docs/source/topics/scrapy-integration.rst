==============================
Using the Frontier with Scrapy
==============================

Using Frontera is quite easy, it includes a set of `Scrapy middlewares`_  that encapsulates frontier usage and
can be easily configured using `Scrapy settings`_.


Activating the frontier
=======================

The frontier uses 2 different middlewares: ``CrawlFrontierSpiderMiddleware`` and ``CrawlFrontierDownloaderMiddleware``.

To activate the frontier in your Scrapy project, just add them to the `SPIDER_MIDDLEWARES`_  and
`DOWNLOADER_MIDDLEWARES`_ settings::

    SPIDER_MIDDLEWARES.update({
        'frontera.contrib.scrapy.middlewares.frontier.CrawlFrontierSpiderMiddleware': 1000,
    })

    DOWNLOADER_MIDDLEWARES.update({
        'frontera.contrib.scrapy.middlewares.frontier.CrawlFrontierDownloaderMiddleware': 1000,
    })

Create a Frontera ``settings.py`` file and add it to your Scrapy settings::

    FRONTIER_SETTINGS = 'tutorial/frontier/settings.py'



Organizing files
================

When using frontier with a Scrapy project, we propose the following directory structure::

    my_scrapy_project/
        my_scrapy_project/
            frontier/
                __init__.py
                settings.py
                middlewares.py
                backends.py
            spiders/
                ...
            __init__.py
            settings.py
         scrapy.cfg

These are basically:

- ``my_scrapy_project/frontier/settings.py``: the frontier settings file.
- ``my_scrapy_project/frontier/middlewares.py``: the middlewares used by the frontier.
- ``my_scrapy_project/frontier/backends.py``: the backend(s) used by the frontier.
- ``my_scrapy_project/spiders``: the Scrapy spiders folder
- ``my_scrapy_project/settings.py``: the Scrapy settings file
- ``scrapy.cfg``: the Scrapy config file

Running the Crawl
=================

Just run your Scrapy spider as usual from the command line::

    scrapy crawl myspider

In case you need to disable frontier, you can do it by overriding the :setting:`FRONTIER_ENABLED <FRONTIER_ENABLED>`
setting::

    scrapy crawl myspider -s FRONTIER_ENABLED=False


Frontier Scrapy settings
========================

Here’s a list of all available Frontera Scrapy settings, in alphabetical order, along with their default values
and the scope where they apply:

.. setting:: FRONTIER_ENABLED

FRONTIER_ENABLED
----------------

Default: ``True``

Whether to enable frontier in your Scrapy project.

.. setting:: FRONTIER_SCHEDULER_CONCURRENT_REQUESTS

FRONTIER_SCHEDULER_CONCURRENT_REQUESTS
--------------------------------------

Default: ``256``

Number of concurrent requests that the middleware will maintain while asking for next pages.

.. setting:: FRONTIER_SCHEDULER_INTERVAL

FRONTIER_SCHEDULER_INTERVAL
---------------------------

Default: ``0.01``

Interval of number of requests check in seconds. Indicates how often the frontier will be asked for new pages if
there is gap for new requests.

.. setting:: FRONTIER_SETTINGS

FRONTIER_SETTINGS
-----------------

Default: ``None``

A file path pointing to Frontera settings.

.. _Scrapy middlewares: http://doc.scrapy.org/en/latest/topics/downloader-middleware.html
.. _Scrapy settings: http://doc.scrapy.org/en/latest/topics/settings.html
.. _DOWNLOADER_MIDDLEWARES: http://doc.scrapy.org/en/latest/topics/settings.html#std:setting-DOWNLOADER_MIDDLEWARES
.. _SPIDER_MIDDLEWARES: http://doc.scrapy.org/en/latest/topics/settings.html#std:setting-SPIDER_MIDDLEWARES
