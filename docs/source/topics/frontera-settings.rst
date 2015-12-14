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


.. setting:: EVENT_LOGGER

EVENT_LOGGER
------------

Default: ``'frontera.logger.events.EventLogManager'``

The EventLoggerManager class to be used by the Frontier.


.. setting:: LOGGER

LOGGER
------

Default: ``'frontera.logger.FrontierLogger'``

The Logger class to be used by the Frontier.

.. setting:: MAX_NEXT_REQUESTS

MAX_NEXT_REQUESTS
-----------------

Default: ``64``

The maximum number of requests returned by
:attr:`get_next_requests <frontera.core.manager.FrontierManager.get_next_requests>` API method.
If value is 0 (default), no maximum value will be used.

.. setting:: MAX_REQUESTS

MAX_REQUESTS
------------

Default: ``0``

Maximum number of returned requests after which Frontera is finished.
If value is 0 (default), the frontier will continue indefinitely. See :ref:`Finishing the frontier <frontier-finish>`.


.. setting:: MIDDLEWARES

MIDDLEWARES
-----------

A list containing the middlewares enabled in the frontier. For more info see
:ref:`Activating a middleware <frontier-activating-middleware>`.

Default::

    [
        'frontera.contrib.middlewares.fingerprint.UrlFingerprintMiddleware',
    ]

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


.. setting:: TEST_MODE

TEST_MODE
---------

Default: ``False``

Whether to enable frontier test mode. See :ref:`Frontier test mode <frontier-test-mode>`

.. setting:: OVERUSED_SLOT_FACTOR

OVERUSED_SLOT_FACTOR
--------------------

Default: ``5.0``

(in progress + queued requests in that slot) / max allowed concurrent downloads per slot before slot is considered
overused. This affects only Scrapy scheduler."

.. setting:: DELAY_ON_EMPTY

DELAY_ON_EMPTY
--------------

Default: ``0.0``

When backend has no requests to fetch, this delay helps to exhaust the rest of the buffer without hitting
backend on every request. Increase it if calls to your backend is taking a lot of time, and decrease if you need a fast
spider bootstrap from seeds. Keep in mind, this setting prevents Frontera from fetching new requests from backend,
therefore causing Scrapy spider to close prematurely. To prevent spider from closing you can either use DontCloseSpider
exception raising from `spider_idle <http://doc.scrapy.org/en/latest/topics/signals.html#spider-idle>`_ signal, or
keeping spider queue always full.


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

.. setting:: SPIDER_FEED_PARTITIONS

SPIDER_FEED_PARTITIONS
^^^^^^^^^^^^^^^^^^^^^^

Default: ``1``

Number of partitions to use in backend. This option is useful when backend is used in distributed environment, and
meant to be equal to number of fetching processes used in your cluster.

.. setting:: STATE_CACHE_SIZE

STATE_CACHE_SIZE
^^^^^^^^^^^^^^^^

Default: ``1000000``

Maximum count of elements in state cache before it gets clear.


SQLAlchemy
----------

.. setting:: SQLALCHEMYBACKEND_CACHE_SIZE

SQLALCHEMYBACKEND_CACHE_SIZE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Default: ``10000``

SQLAlchemy Metadata LRU Cache size. It's used for caching objects, which are requested from DB every time already known,
documents are crawled. This is mainly saves DB throughput, increase it if you're experiencing problems with too high
volume of SELECT's to Metadata table, or decrease if you need to save memory.

.. setting::

SQLALCHEMYBACKEND_CLEAR_CONTENT
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Default: ``True``

Set to ``False`` if you need to disable table content clean up on backend instantiation (e.g. every Scrapy spider run).


.. setting::

SQLALCHEMYBACKEND_DROP_ALL_TABLES
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Default: ``True``

Set to ``False`` if you need to disable dropping of DB tables on backend instantiation (e.g. every Scrapy spider run).

.. setting::

SQLALCHEMYBACKEND_ENGINE
^^^^^^^^^^^^^^^^^^^^^^^^

Default:: ``sqlite:///:memory:``

SQLAlchemy database URL. Default is set to memory.

.. setting::

SQLALCHEMYBACKEND_ENGINE_ECHO
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Default: ``False``

Turn on/off SQLAlchemy verbose output. Useful for debugging SQL queries.

.. setting::

SQLALCHEMYBACKEND_MODELS
^^^^^^^^^^^^^^^^^^^^^^^^

Default::

    {
        'MetadataModel': 'frontera.contrib.backends.sqlalchemy.models.MetadataModel',
        'StateModel': 'frontera.contrib.backends.sqlalchemy.models.StateModel',
        'QueueModel': 'frontera.contrib.backends.sqlalchemy.models.QueueModel'
    }

This is mapping with SQLAlchemy models used by backends. It is mainly used for customization.


Revisting backend
-----------------

.. setting:: SQLALCHEMYBACKEND_REVISIT_INTERVAL

SQLALCHEMYBACKEND_REVISIT_INTERVAL
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Default: ``timedelta(days=1)``

Time between document visits, expressed in ``datetime.timedelta`` objects. Changing of this setting will only affect
documents scheduled after the change. All previously queued documents will be crawled with old periodicity.


Default settings
================

If no settings are specified, frontier will use the built-in default ones. For a complete list of default values see:
:ref:`Built-in settings reference <frontier-built-in-frontier-settings>`. All default settings can be overridden.


Logging default settings
------------------------

Values::

    LOGGER = 'frontera.logger.FrontierLogger'
    LOGGING_ENABLED = True

    LOGGING_EVENTS_ENABLED = False
    LOGGING_EVENTS_INCLUDE_METADATA = True
    LOGGING_EVENTS_INCLUDE_DOMAIN = True
    LOGGING_EVENTS_INCLUDE_DOMAIN_FIELDS = ['name', 'netloc', 'scheme', 'sld', 'tld', 'subdomain']
    LOGGING_EVENTS_HANDLERS = [
        "frontera.logger.handlers.COLOR_EVENTS",
    ]

    LOGGING_MANAGER_ENABLED = False
    LOGGING_MANAGER_LOGLEVEL = logging.DEBUG
    LOGGING_MANAGER_HANDLERS = [
        "frontera.logger.handlers.COLOR_CONSOLE_MANAGER",
    ]

    LOGGING_BACKEND_ENABLED = False
    LOGGING_BACKEND_LOGLEVEL = logging.DEBUG
    LOGGING_BACKEND_HANDLERS = [
        "frontera.logger.handlers.COLOR_CONSOLE_BACKEND",
    ]

    LOGGING_DEBUGGING_ENABLED = False
    LOGGING_DEBUGGING_LOGLEVEL = logging.DEBUG
    LOGGING_DEBUGGING_HANDLERS = [
        "frontera.logger.handlers.COLOR_CONSOLE_DEBUGGING",
    ]

    EVENT_LOG_MANAGER = 'frontera.logger.events.EventLogManager'

