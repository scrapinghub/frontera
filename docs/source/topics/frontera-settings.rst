========
Settings
========

The Frontera settings allows you to customize the behaviour of all components, including the
:class:`FrontierManager <frontera.core.manager.FrontierManager>`,
:class:`Middleware <frontera.core.components.Middleware>` and
:class:`Backend <frontera.core.components.Backend>` themselves.

The infrastructure of the settings provides a global namespace of key-value mappings that can be used to pull
configuration values from. The settings can be populated through different mechanisms, which are described below.

For a list of available built-in settings see: :ref:`Built-in settings reference <frontier-built-in-frontier-settings>`.

Designating the settings
========================

When you use Frontera, you have to tell it which settings you’re using. As
:class:`FrontierManager <frontera.core.manager.FrontierManager>` is the main entry point to Frontier usage,
you can do this by using the method described in the :ref:`Loading from settings <frontier-loading-from-settings>`
section.

When using a string path pointing to a settings file for the frontier we propose the following directory structure::

    my_project/
        frontier/
            __init__.py
            settings.py
            middlewares.py
            backends.py
        ...

These are basically:

- ``frontier/settings.py``: the frontier settings file.
- ``frontier/middlewares.py``: the middlewares used by the frontier.
- ``frontier/backends.py``: the backend(s) used by the frontier.


How to access settings
======================
:class:`Settings <frontera.settings.Settings>` can be accessed through the
:attr:`FrontierManager.settings <frontera.core.manager.FrontierManager.settings>` attribute, that is passed to
:attr:`Middleware.from_manager <frontera.core.components.Middleware.from_manager>` and
:attr:`Backend.from_manager <frontera.core.components.Backend.from_manager>` class methods::

    class MyMiddleware(Component):

        @classmethod
        def from_manager(cls, manager):
            manager = crawler.settings
            if settings.TEST_MODE:
                print "test mode is enabled!"

In other words, settings can be accessed as attributes of the
:class:`Settings <frontera.settings.Settings>` object.

Settings class
==============

.. autoclass:: frontera.settings.Settings

.. _frontier-built-in-frontier-settings:

Built-in frontier settings
==========================

Here’s a list of all available Frontera settings, in alphabetical order, along with their default values and the
scope where they apply.

.. setting:: AUTO_START

AUTO_START
----------

Default: ``True``

Whether to enable frontier automatic start. See :ref:`Starting/Stopping the frontier <frontier-start-stop>`

.. setting:: BACKEND

BACKEND
-------

Default: ``'frontera.contrib.backends.memory.FIFO'``

The :class:`Backend <frontera.core.components.Backend>` to be used by the frontier. For more info see
:ref:`Activating a backend <frontier-activating-backend>`.

.. setting:: CANONICAL_SOLVER

CANONICAL_SOLVER
----------------

Default: ``frontera.contrib.canonicalsolvers.Basic``

The :class:`CanonicalSolver <frontera.core.components.CanonicalSolver>` to be used by the frontier for resolving
canonical URLs. For more info see :ref:`Canonical URL Solver <canonical-url-solver>`.

.. setting:: CONSUMER_BATCH_SIZE

CONSUMER_BATCH_SIZE
-------------------

Default: ``512``

This is a batch size used by strategy and db workers for consuming of spider log and scoring log streams. Increasing it
will cause worker to spend more time on every task, but processing more items per task, therefore leaving less time for
other tasks during some fixed time interval. Reducing it will result to running several tasks withing the same time
interval, but with less overall efficiency. Use it when your consumers too slow, or too fast.

.. setting:: CRAWLING_STRATEGY

CRAWLING_STRATEGY
-----------------

Default: ``None``

The path to crawling strategy class, instantiated and used in :term:`strategy worker` to prioritize and stop crawling in
distributed run mode.

.. setting:: DELAY_ON_EMPTY

DELAY_ON_EMPTY
--------------

Default: ``5.0``

Delay between calls to backend for new batches in Scrapy scheduler, when queue size is getting below
``CONCURRENT_REQUESTS``. When backend has no requests to fetch, this delay helps to exhaust the rest of the buffer
without hitting backend on every request. Increase it if calls to your backend is taking too long, and decrease
if you need a fast spider bootstrap from seeds.

.. setting:: KAFKA_GET_TIMEOUT

KAFKA_GET_TIMEOUT
-----------------

Default: ``5.0``

Time process should block until requested amount of data will be received from message bus.

.. setting:: LOGGING_CONFIG

LOGGING_CONFIG
--------------

Default: ``logging.conf``

The path to a file with logging module configuration. See
https://docs.python.org/2/library/logging.config.html#logging-config-fileformat If file is absent, the logging system
will be initialized with ``logging.basicConfig()`` and CONSOLE handler will be used. This option is used only in
:term:`db worker` and :term:`strategy worker`.

.. setting:: MAX_NEXT_REQUESTS

MAX_NEXT_REQUESTS
-----------------

Default: ``64``

The maximum number of requests returned by
:attr:`get_next_requests <frontera.core.manager.FrontierManager.get_next_requests>` API method. In distributed context
it could be amount of requests produced per spider by :term:`db worker` or count of requests read from message bus per
attempt to fill the spider queue. In single process it's the count of requests to get from backend per one call to
``get_next_requests`` method.


.. setting:: MAX_REQUESTS

MAX_REQUESTS
------------

Default: ``0``

Maximum number of returned requests after which Frontera is finished.
If value is 0 (default), the frontier will continue indefinitely. See :ref:`Finishing the frontier <frontier-finish>`.

.. setting:: MESSAGE_BUS

MESSAGE_BUS
-----------

Default: ``frontera.contrib.messagebus.zeromq.MessageBus``

Points Frontera to :term:`message bus` implementation. Defaults to ZeroMQ.

.. setting:: MIDDLEWARES

MIDDLEWARES
-----------

A list containing the middlewares enabled in the frontier. For more info see
:ref:`Activating a middleware <frontier-activating-middleware>`.

Default::

    [
        'frontera.contrib.middlewares.fingerprint.UrlFingerprintMiddleware',
    ]

.. setting:: NEW_BATCH_DELAY

NEW_BATCH_DELAY
---------------

Default: ``30.0``

Used in DB worker, and it's a time interval between production of new batches for all partitions. If partition is busy,
it will be skipped.

.. setting:: OVERUSED_SLOT_FACTOR

OVERUSED_SLOT_FACTOR
--------------------

Default: ``5.0``

(in progress + queued requests in that slot) / max allowed concurrent downloads per slot before slot is considered
overused. This affects only Scrapy scheduler."

.. setting:: REQUEST_MODEL

REQUEST_MODEL
-------------

Default: ``'frontera.core.models.Request'``

The :class:`Request <frontera.core.models.Request>` model to be used by the frontier.


.. setting:: RESPONSE_MODEL

RESPONSE_MODEL
--------------

Default: ``'frontera.core.models.Response'``

The :class:`Response <frontera.core.models.Response>` model to be used by the frontier.


.. setting:: SCORING_PARTITION_ID

SCORING_PARTITION_ID
--------------------

Default: ``0``

Used by strategy worker, and represents partition startegy worker assigned to.


.. setting:: SPIDER_LOG_PARTITIONS

SPIDER_LOG_PARTITIONS
---------------------

Default: ``1``

Number of :term:`spider log` stream partitions. This affects number of required :term:`strategy worker` (s),
each strategy worker assigned to it's own partition.

.. setting:: SPIDER_FEED_PARTITIONS

SPIDER_FEED_PARTITIONS
----------------------

Default: ``1``

Number of :term:`spider feed` partitions. This directly affects number of spider processes running. Every spider is
assigned to it's own partition.


.. setting:: SPIDER_PARTITION_ID

SPIDER_PARTITION_ID
-------------------

Default: ``0``

Per-spider setting, pointing spider to it's assigned partition.

.. setting:: STATE_CACHE_SIZE

STATE_CACHE_SIZE
----------------

Default: ``1000000``

Maximum count of elements in state cache before it gets clear.

.. setting:: STORE_CONTENT

STORE_CONTENT
-------------

Default: ``False``

Determines if content should be sent over the message bus and stored in the backend: a serious performance killer.

.. setting:: TEST_MODE

TEST_MODE
---------

Default: ``False``

Whether to enable frontier test mode. See :ref:`Frontier test mode <frontier-test-mode>`




Built-in fingerprint middleware settings
========================================

Settings used by the :ref:`UrlFingerprintMiddleware <frontier-url-fingerprint-middleware>` and
:ref:`DomainFingerprintMiddleware <frontier-domain-fingerprint-middleware>`.

.. _frontier-default-settings:

.. setting:: URL_FINGERPRINT_FUNCTION

URL_FINGERPRINT_FUNCTION
------------------------

Default: ``frontera.utils.fingerprint.sha1``

The function used to calculate the ``url`` fingerprint.


.. setting:: DOMAIN_FINGERPRINT_FUNCTION

DOMAIN_FINGERPRINT_FUNCTION
---------------------------

Default: ``frontera.utils.fingerprint.sha1``

The function used to calculate the ``domain`` fingerprint.


.. setting:: TLDEXTRACT_DOMAIN_INFO

TLDEXTRACT_DOMAIN_INFO
----------------------

Default: ``False``

If set to ``True``, will use `tldextract`_ to attach extra domain information
(second-level, top-level and subdomain) to meta field (see :ref:`frontier-objects-additional-data`).


.. _tldextract: https://pypi.python.org/pypi/tldextract


Built-in backends settings
==========================

SQLAlchemy
----------

.. setting:: SQLALCHEMYBACKEND_CACHE_SIZE

SQLALCHEMYBACKEND_CACHE_SIZE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Default: ``10000``

SQLAlchemy Metadata LRU Cache size. It's used for caching objects, which are requested from DB every time already known,
documents are crawled. This is mainly saves DB throughput, increase it if you're experiencing problems with too high
volume of SELECT's to Metadata table, or decrease if you need to save memory.

.. setting:: SQLALCHEMYBACKEND_CLEAR_CONTENT

SQLALCHEMYBACKEND_CLEAR_CONTENT
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Default: ``True``

Set to ``False`` if you need to disable table content clean up on backend instantiation (e.g. every Scrapy spider run).


.. setting:: SQLALCHEMYBACKEND_DROP_ALL_TABLES

SQLALCHEMYBACKEND_DROP_ALL_TABLES
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Default: ``True``

Set to ``False`` if you need to disable dropping of DB tables on backend instantiation (e.g. every Scrapy spider run).

.. setting:: SQLALCHEMYBACKEND_ENGINE

SQLALCHEMYBACKEND_ENGINE
^^^^^^^^^^^^^^^^^^^^^^^^

Default:: ``sqlite:///:memory:``

SQLAlchemy database URL. Default is set to memory.

.. setting:: SQLALCHEMYBACKEND_ENGINE_ECHO

SQLALCHEMYBACKEND_ENGINE_ECHO
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Default: ``False``

Turn on/off SQLAlchemy verbose output. Useful for debugging SQL queries.

.. setting:: SQLALCHEMYBACKEND_MODELS

SQLALCHEMYBACKEND_MODELS
^^^^^^^^^^^^^^^^^^^^^^^^

Default::

    {
        'MetadataModel': 'frontera.contrib.backends.sqlalchemy.models.MetadataModel',
        'StateModel': 'frontera.contrib.backends.sqlalchemy.models.StateModel',
        'QueueModel': 'frontera.contrib.backends.sqlalchemy.models.QueueModel'
    }

This is mapping with SQLAlchemy models used by backends. It is mainly used for customization.


Revisiting backend
------------------

.. setting:: SQLALCHEMYBACKEND_REVISIT_INTERVAL

SQLALCHEMYBACKEND_REVISIT_INTERVAL
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Default: ``timedelta(days=1)``

Time between document visits, expressed in ``datetime.timedelta`` objects. Changing of this setting will only affect
documents scheduled after the change. All previously queued documents will be crawled with old periodicity.


.. _hbase-settings:

HBase backend
-------------

.. setting:: HBASE_BATCH_SIZE

HBASE_BATCH_SIZE
^^^^^^^^^^^^^^^^

Default: ``9216``

Count of accumulated PUT operations before they sent to HBase.

.. setting:: HBASE_DROP_ALL_TABLES

HBASE_DROP_ALL_TABLES
^^^^^^^^^^^^^^^^^^^^^

Default: ``False``

Enables dropping and creation of new HBase tables on worker start.

.. setting:: HBASE_METADATA_TABLE

HBASE_METADATA_TABLE
^^^^^^^^^^^^^^^^^^^^

Default: ``metadata``

Name of the documents metadata table.

.. setting:: HBASE_NAMESPACE

HBASE_NAMESPACE
^^^^^^^^^^^^^^^

Default: ``crawler``

Name of HBase namespace where all crawler related tables will reside.

.. setting:: HBASE_QUEUE_TABLE

HBASE_QUEUE_TABLE
^^^^^^^^^^^^^^^^^

Default: ``queue``

Name of HBase priority queue table.

.. setting:: HBASE_STATE_CACHE_SIZE_LIMIT

HBASE_STATE_CACHE_SIZE_LIMIT
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Default: ``3000000``

Number of items in the :term:`state cache` of :term:`strategy worker`, before it get's flushed to HBase and cleared.

.. setting:: HBASE_THRIFT_HOST

HBASE_THRIFT_HOST
^^^^^^^^^^^^^^^^^

Default: ``localhost``

HBase Thrift server host.

.. setting:: HBASE_THRIFT_PORT

HBASE_THRIFT_PORT
^^^^^^^^^^^^^^^^^

Default: ``9090``

HBase Thrift server port

.. setting:: HBASE_USE_FRAMED_COMPACT

HBASE_USE_FRAMED_COMPACT
^^^^^^^^^^^^^^^^^^^^^^^^

Default: ``False``

Enabling this option dramatically reduces transmission overhead, but the server needs to be properly configured to use
Thrifts framed transport and compact protocol.

.. setting:: HBASE_USE_SNAPPY

HBASE_USE_SNAPPY
^^^^^^^^^^^^^^^^

Default: ``False``

Whatever to compress content and metadata in HBase using Snappy. Decreases amount of disk and network IO within HBase,
lowering response times. HBase have to be properly configured to support Snappy compression.

.. _zeromq-settings:

ZeroMQ message bus settings
===========================

The message bus class is ``distributed_frontera.messagebus.zeromq.MessageBus``

.. setting:: ZMQ_ADDRESS

ZMQ_ADDRESS
-----------

Default: ``127.0.0.1``

Defines where the ZeroMQ socket should bind or connect. Can be a hostname or an IP
address. Right now ZMQ has only been properly tested with IPv4. Proper IPv6
support will be added in the near future.

.. setting:: ZMQ_BASE_PORT

ZMQ_BASE_PORT
-------------

Default: ``5550``

The base port for all ZeroMQ sockets. It uses 6 sockets overall and port starting from base with step 1. Be sure that
interval [base:base+5] is available.

.. _kafka-settings:

Kafka message bus settings
==========================

The message bus class is ``frontera.contrib.messagebus.kafkabus.MessageBus``

.. setting:: KAFKA_LOCATION

KAFKA_LOCATION
--------------

Hostname and port of kafka broker, separated with :. Can be a string with hostname:port pair separated with commas(,).

.. setting:: FRONTIER_GROUP

FRONTIER_GROUP
--------------

Default: ``general``

Kafka consumer group name, used for almost everything.


.. setting:: INCOMING_TOPIC

INCOMING_TOPIC
--------------

Default: ``frontier-done``

Spider log stream topic name.


.. setting:: OUTGOING_TOPIC

OUTGOING_TOPIC
--------------

Default: ``frontier-todo``

Spider feed stream topic name.


.. setting:: SCORING_GROUP

SCORING_GROUP
-------------

Default: ``strategy-workers``

A group used by strategy workers for spider log reading. Needs to be different than ``FRONTIER_GROUP``.

.. setting:: SCORING_TOPIC

SCORING_TOPIC
-------------

Kafka topic used for :term:`scoring log` stream.


Default settings
================

If no settings are specified, frontier will use the built-in default ones. For a complete list of default values see:
:ref:`Built-in settings reference <frontier-built-in-frontier-settings>`. All default settings can be overridden.

