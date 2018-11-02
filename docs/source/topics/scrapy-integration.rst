==============================
Using the Frontier with Scrapy
==============================

To use Frontera with Scrapy, you will need to add `Scrapy middlewares`_ and redefine the default Scrapy scheduler with
custom Frontera scheduler. Both can be done by modifying `Scrapy settings`_.


The purpose
===========

Scrapy is expected to be used as a fetching, HTML parsing and links extracting component. Your spider code have
 to produce responses and requests from extracted links. That's all. Frontera's business is to keep the links, queue
and schedule links when needed.

Please make sure all the middlewares affecting the crawling, like DepthMiddleware, OffsiteMiddleware or
RobotsTxtMiddleware are disabled.

All other use cases when Scrapy is busy items generation, scraping from HTML, scheduling links directly trying to bypass
Frontera, are doomed to cause countless hours of maintenance. Please don't use Frontera integrated with Scrapy that way.


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
            spiders/
                ...
            __init__.py
            settings.py
         scrapy.cfg

These are basically:

- ``my_scrapy_project/frontera/settings.py``: the Frontera settings file.
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

Creation of new Scrapy project is described at `Quick start single process`_ page. Again, your spider code have
 to produce responses and requests from extracted links. Also, make sure exceptions caused by request processing are
not intercepted by any of the middlewares. Otherwise errors delivery to :term:`crawling strategy` will be broken.

Here is an example code to start::

    from scrapy import Spider
    from scrapy.linkextractors import LinkExtractor
    from scrapy.http import Request
    from scrapy.http.response.html import HtmlResponse

    class CommonPageSpider(Spider):

        name = "commonpage"

        def __init__(self, *args, **kwargs):
            super(CommonPageSpider, self).__init__(*args, **kwargs)
            self.le = LinkExtractor()

        def parse(self, response):
            if not isinstance(response, HtmlResponse):
                return
            for link in self.le.extract_links(response):
                r = Request(url=link.url)
                r.meta.update(link_text=link.text)
                yield r



Configuration guidelines
------------------------

Please specify a correct user agent string to disclose yourself to webmasters::

    USER_AGENT = 'Some-Bot (+http://url/to-the-page-describing-the-purpose-of-crawling)'


When using Frontera robots.txt obeying have to be implemented in :term:`crawling strategy`::

    ROBOTSTXT_OBEY = False

Disable some of the spider and downloader middlewares which may affect the crawling::

    SPIDER_MIDDLEWARES.update({
        'scrapy.spidermiddlewares.offsite.OffsiteMiddleware': None,
        'scrapy.spidermiddlewares.referer.RefererMiddleware': None,
        'scrapy.spidermiddlewares.urllength.UrlLengthMiddleware': None,
        'scrapy.spidermiddlewares.depth.DepthMiddleware': None,
        'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware': None
    })

    DOWNLOADER_MIDDLEWARES.update({
        'scrapy.downloadermiddlewares.httpauth.HttpAuthMiddleware': None,
    })

    del DOWNLOADER_MIDDLEWARES_BASE['scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware']


There several tunings you can make for efficient broad crawling.

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
.. _`Scrapy Middleware doc`: http://doc.scrapy.org/en/latest/topics/spider-middleware.html
