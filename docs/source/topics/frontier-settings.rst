========
Settings
========

The Crawl Frontier settings allows you to customize the behaviour of all components, including the
:class:`FrontierManager`, :class:`Middleware` and :class:`Backend` themselves.

The infrastructure of the settings provides a global namespace of key-value mappings that can be used to pull
configuration values from. The settings can be populated through different mechanisms, which are described below.

For a list of available built-in settings see: :ref:`Built-in settings reference <frontier-built-in-frontier-settings>`.

Designating the settings
========================

When you use Crawl Frontier, you have to tell it which settings you’re using. As :class:`FrontierManager` is the main
entry point to Frontier usage, you can do this by using the method described in the
:ref:`Loading from settings <frontier-loading-from-settings>` section.

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
:class:`Settings` can be accessed through the ``settings`` attribute of the :class:`FrontierManager`, that is passed to
``from_manager`` class method in :class:`Middleware` and :class:`Backend` objects::

    class MyMiddleware(Component):

        @classmethod
        def from_manager(cls, manager):
            manager = crawler.settings
            if settings.TEST_MODE:
                print "test mode is enabled!"

In other words, settings can be accessed as attributes of the :class:`Settings` object.

.. _frontier-built-in-frontier-settings:

Built-in frontier settings
==========================

Here’s a list of all available Crawl Frontier settings, in alphabetical order, along with their default values and the
scope where they apply.

.. setting:: AUTO_START
AUTO_START
----------

Default: ``True``

Whether to enable frontier automatic start. See :ref:`Starting/Stopping the frontier <frontier-start-stop>`

.. setting:: BACKEND
BACKEND
-------

Default: ``'crawlfrontier.contrib.backends.memory.heapq.FIFO'``

The :class:`Backend` to be used by the frontier. For more info see
:ref:`Activating a backend <frontier-activating-backend>`.

.. setting:: LINK_MODEL
LINK_MODEL
----------

Default: ``'crawlfrontier.core.models.Link'``

The :class:`Link` model to be used by the frontier.

.. setting:: MAX_NEXT_PAGES
MAX_NEXT_PAGES
--------------

Default: ``0``

The maximum number of pages returned by ``get_next_pages`` API method. If value is 0 (default), no maximum value will
be used.

.. setting:: MAX_PAGES
MAX_PAGES
--------------

Default: ``0``

Maximum number of returned pages after Crawl frontier is finished. If value is 0 (default), the frontier will continue
indefinitely. See :ref:`Finishing the frontier <frontier-finish>`.


.. setting:: MIDDLEWARES
MIDDLEWARES
--------------------

A list containing the middlewares enabled in the frontier. For more info see
:ref:`Activating a middleware <frontier-activating-middleware>`.

Default::

    [
        'crawlfrontier.contrib.middlewares.domain.DomainMiddleware',
        'crawlfrontier.contrib.middlewares.fingerprint.UrlFingerprintMiddleware',
        'crawlfrontier.contrib.middlewares.fingerprint.DomainFingerprintMiddleware',
    ]

.. setting:: REQUEST_MODEL

REQUEST_MODEL
-------------

Default: ``'crawlfrontier.core.models.Request'``

The :class:`Request` model to be used by the frontier.


.. setting:: RESPONSE_MODEL

RESPONSE_MODEL
-------------

Default: ``'crawlfrontier.core.models.Request'``

The :class:`Request` model to be used by the frontier.


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

Default: ``crawlfrontier.utils.fingerprint.sha1``

The function used to calculate the ``url`` fingerprint.


.. setting:: DOMAIN_FINGERPRINT_FUNCTION

DOMAIN_FINGERPRINT_FUNCTION
------------------------

Default: ``crawlfrontier.utils.fingerprint.sha1``

The function used to calculate the ``domain`` fingerprint.




Default settings
================

If no settings are specified, frontier will use the built-in default ones. For a complete list of default values see:
:ref:`Built-in settings reference <frontier-built-in-frontier-settings>`. All default settings can be overridden.

Frontier default settings
-------------------------

Values::

    PAGE_MODEL = 'crawlfrontier.core.models.Page'
    LINK_MODEL = 'crawlfrontier.core.models.Link'
    FRONTIER = 'crawlfrontier.core.frontier.Frontier'
    MIDDLEWARES = [
        'crawlfrontier.contrib.middlewares.domain.DomainMiddleware',
        'crawlfrontier.contrib.middlewares.fingerprint.UrlFingerprintMiddleware',
        'crawlfrontier.contrib.middlewares.fingerprint.DomainFingerprintMiddleware',
    ]
    BACKEND = 'crawlfrontier.contrib.backends.memory.heapq.FIFO'
    TEST_MODE = False
    MAX_PAGES = 0
    MAX_NEXT_PAGES = 0
    AUTO_START = True

Fingerprints middleware default settings
----------------------------------------

Values::

    URL_FINGERPRINT_FUNCTION = 'crawlfrontier.utils.fingerprint.sha1'
    DOMAIN_FINGERPRINT_FUNCTION = 'crawlfrontier.utils.fingerprint.sha1'


Logging default settings
------------------------

Values::

    LOGGER = 'crawlfrontier.logger.FrontierLogger'
    LOGGING_ENABLED = True

    LOGGING_EVENTS_ENABLED = False
    LOGGING_EVENTS_INCLUDE_METADATA = True
    LOGGING_EVENTS_INCLUDE_DOMAIN = True
    LOGGING_EVENTS_INCLUDE_DOMAIN_FIELDS = ['name', 'netloc', 'scheme', 'sld', 'tld', 'subdomain']
    LOGGING_EVENTS_HANDLERS = [
        "crawlfrontier.logger.handlers.COLOR_EVENTS",
    ]

    LOGGING_MANAGER_ENABLED = False
    LOGGING_MANAGER_LOGLEVEL = logging.DEBUG
    LOGGING_MANAGER_HANDLERS = [
        "crawlfrontier.logger.handlers.COLOR_CONSOLE_MANAGER",
    ]

    LOGGING_BACKEND_ENABLED = False
    LOGGING_BACKEND_LOGLEVEL = logging.DEBUG
    LOGGING_BACKEND_HANDLERS = [
        "crawlfrontier.logger.handlers.COLOR_CONSOLE_BACKEND",
    ]

    LOGGING_DEBUGGING_ENABLED = False
    LOGGING_DEBUGGING_LOGLEVEL = logging.DEBUG
    LOGGING_DEBUGGING_HANDLERS = [
        "crawlfrontier.logger.handlers.COLOR_CONSOLE_DEBUGGING",
    ]

    EVENT_LOG_MANAGER = 'crawlfrontier.logger.events.EventLogManager'

