============
Seed Loaders
============

Initially the frontier should receive some predefined set of URLs to start the crawl process,
this set of classes is needed to load seed URLs from different sources at system startup.

Generally each seed loader is just an another middleware layer with own logic.

In most cases the seed loader behaviour is defined with:

* some initial procedure on start
* ``process_start_requests()`` method returning an iterable of ``Request`` objects

How to use
----------

To use a seed loader you should add its class to ``SPIDER_MIDDLEWARES`` setting dict::

    SPIDER_MIDDLEWARES.update({
        'crawl_frontier.contrib.scrapy.middlewares.seeds.FileSeedLoader': 650
    })

The value (``650`` here) defines the order for the middleware (check `Scrapy Middleware doc`_).

The description and specific settings for each seed loader class can be found below.


FileSeedLoader
--------------

The simplest seed loader class that can read seed URLs from a local file.

Settings:

* ``SEEDS_SOURCE`` - local file path

S3SeedLoader
------------

Seed loader class that can read seed URLs from Amazon S3 bucket files.

Settings:

* ``SEEDS_SOURCE`` - S3 bucket name::

    SEEDS_SOURCE = "s3://some-project/seed-urls/"

* ``SEEDS_AWS_ACCESS_KEY`` - S3 credentials: access key

* ``SEEDS_AWS_SECRET_ACCESS_KEY`` - S3 credentials: secret access key


.. _`Scrapy Middleware doc`: http://doc.scrapy.org/en/latest/topics/spider-middleware.html
