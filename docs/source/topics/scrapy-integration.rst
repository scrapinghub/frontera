==============================
Using the Frontier with Scrapy
==============================

Using Frontera is quite easy, it includes a set of `Scrapy middlewares`_ and Scrapy scheduler that encapsulates
Frontera usage and can be easily configured using `Scrapy settings`_.


Activating the frontier
=======================

The Frontera uses 2 different middlewares: ``SchedulerSpiderMiddleware`` and ``SchedulerDownloaderMiddleware``, and it's
own scheduler ``FronteraScheduler``.

To activate the Frontera in your Scrapy project, just add them to the `SPIDER_MIDDLEWARES`_,
`DOWNLOADER_MIDDLEWARES`_ and `SCHEDULER`_ settings::

    SPIDER_MIDDLEWARES.update({
        'frontera.contrib.scrapy.middlewares.schedulers.SchedulerSpiderMiddleware': 1000,
    })

    DOWNLOADER_MIDDLEWARES.update({
        'frontera.contrib.scrapy.middlewares.schedulers.SchedulerDownloaderMiddleware': 1000,
    })

    SCHEDULER = 'frontera.contrib.scrapy.schedulers.frontier.FronteraScheduler'

Create a Frontera ``settings.py`` file and add it to your Scrapy settings::

    FRONTERA_SETTINGS = 'tutorial.frontera.settings'

Another option is to put these settings right into Scrapy settings module.



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

Running the сrawl
=================

Just run your Scrapy spider as usual from the command line::

    scrapy crawl myspider


Frontier Scrapy settings
========================
You can configure your frontier two ways:

.. setting:: FRONTERA_SETTINGS

- Using ``FRONTERA_SETTINGS`` parameter, which is a module path pointing to Frontera settings in Scrapy settings file.
  Defaults to ``None``

- Define frontier settings right into Scrapy settings file.


Defining frontier settings via Scrapy settings
----------------------------------------------

:ref:`Frontier settings <frontier-built-in-frontier-settings>` can also be defined via Scrapy settings.
In this case, the order of precedence will be the following:

1. Settings defined in the file pointed by :setting:`FRONTERA_SETTINGS` (higher precedence)
2. Settings defined in the Scrapy settings
3. Default frontier settings


.. _Scrapy middlewares: http://doc.scrapy.org/en/latest/topics/downloader-middleware.html
.. _Scrapy settings: http://doc.scrapy.org/en/latest/topics/settings.html
.. _DOWNLOADER_MIDDLEWARES: http://doc.scrapy.org/en/latest/topics/settings.html#std:setting-DOWNLOADER_MIDDLEWARES
.. _SPIDER_MIDDLEWARES: http://doc.scrapy.org/en/latest/topics/settings.html#std:setting-SPIDER_MIDDLEWARES
.. _SCHEDULER: http://doc.scrapy.org/en/latest/topics/settings.html#std:setting-SCHEDULER


Writing Scrapy spider
=====================

Spider logic
------------
Creation of basic Scrapy spider is described at `Quick start single process`_ page.

It's also a good practice to prevent spider from closing because of insufficiency of queued requests transport:::

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(*args, **kwargs)
        spider._set_crawler(crawler)
        spider.crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def spider_idle(self):
        self.log("Spider idle signal caught.")
        raise DontCloseSpider


Configuration guidelines
------------------------

There several tunings you can make for efficient broad crawling.

Adding one of seed loaders for bootstrapping of crawling process::

    SPIDER_MIDDLEWARES.update({
        'frontera.contrib.scrapy.middlewares.seeds.file.FileSeedLoader': 1,
    })

Various settings suitable for broad crawling::

    HTTPCACHE_ENABLED = False   # Turns off disk cache, which has low hit ratio during broad crawls
    REDIRECT_ENABLED = True
    COOKIES_ENABLED = False
    DOWNLOAD_TIMEOUT = 120
    RETRY_ENABLED = False   # Retries can be handled by Frontera itself, depending on crawling strategy
    DOWNLOAD_MAXSIZE = 10 * 1024 * 1024  # Maximum document size, causes OOM kills if not set
    LOGSTATS_INTERVAL = 10  # Print stats every 10 secs to console

Auto throttling and concurrency settings for polite and responsible crawling:::

    # auto throttling
    AUTOTHROTTLE_ENABLED = True
    AUTOTHROTTLE_DEBUG = False
    AUTOTHROTTLE_MAX_DELAY = 3.0
    AUTOTHROTTLE_START_DELAY = 0.25     # Any small enough value, it will be adjusted during operation by averaging
                                        # with response latencies.
    RANDOMIZE_DOWNLOAD_DELAY = False

    # concurrency
    CONCURRENT_REQUESTS = 256           # Depends on many factors, and should be determined experimentally
    CONCURRENT_REQUESTS_PER_DOMAIN = 10
    DOWNLOAD_DELAY = 0.0

Check also `Scrapy broad crawling`_ recommendations.


.. _`Quick start single process`: http://frontera.readthedocs.org/en/latest/topics/quick-start-single.html
.. _`Scrapy broad crawling`: http://doc.scrapy.org/en/master/topics/broad-crawls.html


Scrapy Seed Loaders
===================

Frontera has some built-in Scrapy middlewares for seed loading.

Seed loaders use the ``process_start_requests`` method to generate requests from a source that are added later to the
:class:`FrontierManager <frontera.core.manager.FrontierManager>`.


Activating a Seed loader
------------------------

Just add the Seed Loader middleware to the ``SPIDER_MIDDLEWARES`` scrapy settings::

    SPIDER_MIDDLEWARES.update({
        'frontera.contrib.scrapy.middlewares.seeds.FileSeedLoader': 650
    })


.. _seed_loader_file:

FileSeedLoader
--------------

Load seed URLs from a file. The file must be formatted contain one URL per line::

    http://www.asite.com
    http://www.anothersite.com
    ...

Yo can disable URLs using the ``#`` character::

    ...
    #http://www.acommentedsite.com
    ...

**Settings**:

- ``SEEDS_SOURCE``: Path to the seeds file


.. _seed_loader_s3:

S3SeedLoader
------------

Load seeds from a file stored in an Amazon S3 bucket

File format should the same one used in :ref:`FileSeedLoader <seed_loader_file>`.

Settings:

- ``SEEDS_SOURCE``: Path to S3 bucket file. eg: ``s3://some-project/seed-urls/``

- ``SEEDS_AWS_ACCESS_KEY``: S3 credentials Access Key

- ``SEEDS_AWS_SECRET_ACCESS_KEY``: S3 credentials Secret Access Key


.. _`Scrapy Middleware doc`: http://doc.scrapy.org/en/latest/topics/spider-middleware.html
