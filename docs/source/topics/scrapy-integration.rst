==============================
Using the Frontier with Scrapy
==============================

Using Frontera is quite easy, it includes a set of `Scrapy middlewares`_  that encapsulates Frontera usage and
can be easily configured using `Scrapy settings`_.


Activating the frontier
=======================

The Frontera uses 2 different middlewares: ``CrawlFrontierSpiderMiddleware`` and ``CrawlFrontierDownloaderMiddleware``.

To activate the Frontera in your Scrapy project, just add them to the `SPIDER_MIDDLEWARES`_  and
`DOWNLOADER_MIDDLEWARES`_ settings::

    SPIDER_MIDDLEWARES.update({
        'frontera.contrib.scrapy.middlewares.frontier.CrawlFrontierSpiderMiddleware': 1000,
    })

    DOWNLOADER_MIDDLEWARES.update({
        'frontera.contrib.scrapy.middlewares.frontier.CrawlFrontierDownloaderMiddleware': 1000,
    })

Create a Frontera ``settings.py`` file and add it to your Scrapy settings::

    FRONTERA_SETTINGS = 'tutorial/frontera/settings.py'



Organizing files
================

When using frontier with a Scrapy project, we propose the following directory structure::

    my_scrapy_project/
        my_scrapy_project/
            frontera/
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

- ``my_scrapy_project/frontera/settings.py``: the Frontera settings file.
- ``my_scrapy_project/frontera/middlewares.py``: the middlewares used by the Frontera.
- ``my_scrapy_project/frontera/backends.py``: the backend(s) used by the Frontera.
- ``my_scrapy_project/spiders``: the Scrapy spiders folder
- ``my_scrapy_project/settings.py``: the Scrapy settings file
- ``scrapy.cfg``: the Scrapy config file

Running the Crawl
=================

Just run your Scrapy spider as usual from the command line::

    scrapy crawl myspider


Frontier Scrapy settings
========================

Here’s a list of all available Frontera Scrapy settings, in alphabetical order, along with their default values
and the scope where they apply:

.. setting:: FRONTERA_SETTINGS

FRONTERA_SETTINGS
-----------------

Default: ``None``

A file path pointing to Frontera settings.

.. _Scrapy middlewares: http://doc.scrapy.org/en/latest/topics/downloader-middleware.html
.. _Scrapy settings: http://doc.scrapy.org/en/latest/topics/settings.html
.. _DOWNLOADER_MIDDLEWARES: http://doc.scrapy.org/en/latest/topics/settings.html#std:setting-DOWNLOADER_MIDDLEWARES
.. _SPIDER_MIDDLEWARES: http://doc.scrapy.org/en/latest/topics/settings.html#std:setting-SPIDER_MIDDLEWARES
