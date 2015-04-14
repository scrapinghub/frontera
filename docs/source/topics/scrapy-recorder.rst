========================
Recording a Scrapy crawl
========================

Scrapy Recorder is a set of `Scrapy middlewares`_ that will allow you to record a scrapy crawl and store it into a
:doc:`Graph Manager <graph-manager>`.

This can be useful to perform frontier tests without having to crawl the entire site again or even using Scrapy.


Activating the recorder
=======================

The recorder uses 2 different middlewares: ``CrawlRecorderSpiderMiddleware`` and ``CrawlRecorderDownloaderMiddleware``.

To activate the recording in your Scrapy project, just add them to the `SPIDER_MIDDLEWARES`_  and
`DOWNLOADER_MIDDLEWARES`_ settings::

    SPIDER_MIDDLEWARES.update({
        'frontera.contrib.scrapy.middlewares.recording.CrawlRecorderSpiderMiddleware': 1000,
    })

    DOWNLOADER_MIDDLEWARES.update({
        'frontera.contrib.scrapy.middlewares.recording.CrawlRecorderDownloaderMiddleware': 1000,
    })


Choosing your storage engine
============================

As :doc:`Graph Manager <graph-manager>` is internally used by the recorder to store crawled pages, you can choose
between :ref:`different storage engines <graph-manager-database>`.

We can set the storage engine with the :setting:`RECORDER_STORAGE_ENGINE <RECORDER_STORAGE_ENGINE>` setting::

    RECORDER_STORAGE_ENGINE = 'sqlite:///my_record.db'

You can also choose to reset database tables or just reset data with this settings::

    RECORDER_STORAGE_DROP_ALL_TABLES = True
    RECORDER_STORAGE_CLEAR_CONTENT = True

Running the Crawl
=================

Just run your Scrapy spider as usual from the command line::

    scrapy crawl myspider

Once it's finished you should have the recording available and ready for use.

In case you need to disable recording, you can do it by overriding the :setting:`RECORDER_ENABLED <RECORDER_ENABLED>`
setting::

    scrapy crawl myspider -s RECORDER_ENABLED=False

Recorder settings
=================

Here’s a list of all available Scrapy Recorder settings, in alphabetical order, along with their default values and the
scope where they apply.

.. setting:: RECORDER_ENABLED

RECORDER_ENABLED
----------------

Default: ``True``

Activate or deactivate recording middlewares.

.. setting:: RECORDER_STORAGE_CLEAR_CONTENT

RECORDER_STORAGE_CLEAR_CONTENT
------------------------------

Default: ``True``

Deletes table content from :ref:`storage database <graph-manager-database>` in Graph Manager.

.. setting:: RECORDER_STORAGE_DROP_ALL_TABLES

RECORDER_STORAGE_DROP_ALL_TABLES
--------------------------------

Default: ``True``

Drop :ref:`storage database <graph-manager-database>` tables in Graph Manager.

.. setting:: RECORDER_STORAGE_ENGINE

RECORDER_STORAGE_ENGINE
-----------------------

Default: ``None``

Sets :ref:`Graph Manager storage engine <graph-manager-database>` used to store the recording.

.. _Scrapy middlewares: http://doc.scrapy.org/en/latest/topics/downloader-middleware.html
.. _DOWNLOADER_MIDDLEWARES: http://doc.scrapy.org/en/latest/topics/settings.html#std:setting-DOWNLOADER_MIDDLEWARES
.. _SPIDER_MIDDLEWARES: http://doc.scrapy.org/en/latest/topics/settings.html#std:setting-SPIDER_MIDDLEWARES
