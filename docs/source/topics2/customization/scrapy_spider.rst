=====================
Writing Scrapy spider
=====================

Spider logic
============
Creation of basic Scrapy spider is described at `Frontier at a glance`_ page.

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
========================
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


.. _`Frontier at a glance`: http://frontera.readthedocs.org/en/latest/topics/frontier-at-a-glance.html
.. _`Scrapy broad crawling`: http://doc.scrapy.org/en/master/topics/broad-crawls.html