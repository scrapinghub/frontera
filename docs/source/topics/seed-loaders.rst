===================
Scrapy Seed Loaders
===================

Frontera has some built-in Scrapy middlewares for seed loading.

Seed loaders use the ``process_start_requests`` method to generate requests from a source that are added later to the
:class:`FrontierManager <frontera.core.manager.FrontierManager>`.


Activating a Seed loader
------------------------

Just add the Seed Loader middleware to the ``SPIDER_MIDDLEWARES`` scrapy settings::

    SPIDER_MIDDLEWARES.update({
        'crawl_frontier.contrib.scrapy.middlewares.seeds.FileSeedLoader': 650
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
