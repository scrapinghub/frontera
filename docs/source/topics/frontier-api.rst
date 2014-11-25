============
Frontier API
============

This section documents the Crawl Frontier core API, and is intended for developers of middlewares and backends.

Crawl Frontier API / Manager
============================

The main entry point to Crawl Frontier API is the :class:`FrontierManager` object, passed to middlewares and backend
through the from_manager class method. This object provides access to all Crawl Frontier core components, and is the
only way for middlewares and backend to access them and hook their functionality into Crawl Frontier.

The :class:`FrontierManager` is responsible for loading the installed middlewares and backend, as well as for managing
the data flow around the whole frontier.

.. _frontier-loading-from-settings:

Loading from settings
=====================

Although :class:`FrontierManager` can be initialized using parameters the most common way of doing this is using
:doc:`Frontier Settings <frontier-settings>`.

This can be done through the ``from_settings`` class method, using either a string path::

    >>> from crawlfrontier import FrontierManager
    >>> frontier = FrontierManager.from_settings('my_project.frontier.settings')

or a :class:`Settings` object instance::

    >>> from crawlfrontier import FrontierManager, Settings
    >>> settings = Settings()
    >>> settings.MAX_PAGES = 0
    >>> frontier = FrontierManager.from_settings(settings)

It can also be initialized without parameters, in this case the frontier will use the
:ref:`default settings <frontier-default-settings>`::

    >>> from crawlfrontier import FrontierManager, Settings
    >>> frontier = FrontierManager.from_settings()


Frontier Manager
================

.. class:: FrontierManager(page_model, link_model, backend, logger, event_logger, [frontier_middlewares, test_mode, max_pages, max_next_pages, auto_start, settings])

    The :class:`FrontierManager` object encapsulates the whole frontier, providing an API to interact with.
    It's also responsible of loading and communicating all different frontier components.

    :param page_model: The :class:`Page` object to be used by the frontier. Can be defined with :setting:`PAGE_MODEL` setting.
    :type page_model: object/string path

    :param link_model: The :class:`Link` object to be used by the frontier. Can be defined with :setting:`LINK_MODEL` setting.
    :type link_model: object/string path

    :param backend: The :class:`Backend` object to be used by the frontier. Can be defined with :setting:`BACKEND` setting.
    :type backend: object/string path

    :param logger: The :class:`Logger` object to be used by the frontier. Can be defined with :setting:`LOGGER` setting.
    :type logger: object/string path

    :param event_logger: The :class:`EventLogger` object to be used by the frontier. Can be defined with :setting:`EVENT_LOGGER` setting.
    :type event_logger: object/string path

    :param frontier_middlewares: A list of :class:`Middleware` objects to be used by the frontier. Can be defined with :setting:`MIDDLEWARES` setting.
    :type frontier_middlewares: list of objects/string paths=[]

    :param test_mode: Activate/deactivate :ref:`frontier test mode <frontier-test-mode>`. Can be defined with :setting:`TEST_MODE` setting.
    :type test_mode: bool=False

    :param max_pages: Number of pages after which the frontier would stop. See :ref:`Finish conditions <frontier-finish>`. Can be defined with :setting:`MAX_PAGES` setting.
    :type max_pages: int=0

    :param max_next_pages: Maximum number of pages returned by ``get_next_pages`` method. Can be defined with :setting:`MAX_NEXT_PAGES` setting.
    :type max_next_pages: int=0

    :param auto_start: Activate/deactivate automatic frontier start. See :ref:`starting/stopping the frontier <frontier-start-stop>`. Can be defined with :setting:`AUTO_START` setting.
    :type auto_start: bool=True

    :param settings: The :class:`Settings` object used by the frontier.
    :type settings: object=None

    **Attributes**

    .. attribute:: page_model

        :class:`Page` object to be used by the frontier.

    .. attribute:: link_model

        :class:`Link` object to be used by the frontier.

    .. attribute:: backend

        :class:`Backend` object to be used by the frontier.

    .. attribute:: logger

        :class:`Logger` object to be used by the frontier.

    .. attribute:: event_logger

        :class:`EventLogger` object to be used by the frontier.

    .. attribute:: frontier_middlewares

        List of :class:`Middleware` objects to be used by the frontier.

    .. attribute:: test_mode

        Boolean value indicating if the frontier is using :ref:`frontier test mode <frontier-test-mode>`.

    .. attribute:: max_pages

        Number of pages after which the frontier would stop. See :ref:`Finish conditions <frontier-finish>`.

    .. attribute:: max_next_pages

        Maximum number of pages returned by ``get_next_pages`` method.

    .. attribute:: n_pages

        Number of pages returned by the frontier.

    .. attribute:: iteration

        Current :ref:`frontier iteration <frontier-iterations>`.

    .. attribute:: finished

        Boolean value indicating if the frontier has finished. See :ref:`Finish conditions <frontier-finish>`.

    .. attribute:: auto_start

        Boolean value indicating if automatic frontier start is activated. See :ref:`starting/stopping the frontier <frontier-start-stop>`.

    **API Methods**

    .. warning:: In ``page_crawled`` and ``page_crawled_error`` methods, passed :class:`Page` object should be the same one the frontier already returned to be crawled by the ``get_next_pages`` method. Being the same object doesn't necessarily mean using the same instance, it depends on :ref:`how frontier identifies unique pages <frontier-unique-objects>` (usually using the ``fingerprint`` field).

    .. method:: add_seeds(urls)

        Adds a list of seed URLs as entry point for the crawl. Manager will create a list of :class:`Link` objects from
        each URL and pass it to the middlewares and backend. Backend should return a list of :class:`Page` objects

        :param url: A list of seeds URLs.
        :type url: string list

        :return: :class:`Page` list

    .. method:: page_crawled(page, [links])

        Informs the frontier about the page crawl result. Crawl info is passed by using :class:`Page` object fields,
        like status and metadata fields. Extracted links for the current page must be also passed as a list of URLs.

        :param page: The crawled page object
        :type page: :class:`Page`

        :param links: List of URLs extracted from the crawled page.
        :type links: string list=[]

        :return: :class:`Page`


    .. method:: page_crawled_error(page, error)

        Informs the frontier about a page crawl error. An error identifier must be provided.

        :param page: The crawled with error page object.
        :type page: :class:`Page`

        :param error: A string identifier for the error.
        :type error: string

        :return: :class:`Page`


    .. method:: get_next_pages([max_next_pages])

        Returns a list of next pages to be crawled. Optionally a maximum number of pages can be passed, if no value is
        set ``max_next_pages`` attribute is used instead (defined by :setting:`MAX_NEXT_PAGES` setting).

        :param max_next_pages: maximum number of pages to return.
        :type max_next_pages: int

        :return: :class:`Page` list


    .. method:: get_page(url)

        Returns a :class:`Page` object corresponding to the passed URL.

        :param url: the URL of the page.
        :type url: string

        :return: :class:`Page` or None.


    **Manager Methods**

    .. classmethod:: from_settings([settings])

        Returns a :class:`FrontierManager` instance initialized with the passed settings argument. Argument value can
        either be a string path pointing to settings file or a :class:`Settings` object instance. If no settings
        is given, :ref:`frontier default settings <frontier-default-settings>` are used.


    .. method:: start()

        Notifies all the components of the frontier start. Typically used for initializations.
        See :ref:`starting/stopping the frontier <frontier-start-stop>`.


    .. method:: stop()

        Notifies all the components of the frontier stop. Typically used for finalizations.
        See :ref:`starting/stopping the frontier <frontier-start-stop>`.


.. _frontier-start-stop:

Starting/Stopping the frontier
==============================

Sometimes, frontier components need to perform initialization and finalization operations. The frontier mechanism to
notify the different components of the frontier start and stop is done by the ``start()`` and ``stop()`` methods
respectively.

By default ``auto_start`` frontier value is activated, this means that components will be notified once the
:class:`FrontierManager` object is created. If you need to have more fine control of when different components
are initialized, deactivate ``auto_start`` and manually call frontier API ``start()`` and ``stop()`` methods.

.. note:: Frontier ``stop()`` method is not automatically called when ``auto_start`` is active (because frontier is not aware of the crawling state). If you need to notify components of frontier end you should call the method manually.


.. _frontier-iterations:

Frontier iterations
===================

Once frontier is running, the usual process is the one described in the :ref:`data flow <frontier-data-flow>` section.

Crawler asks the frontier for next pages using ``get_next_pages`` method. Each time the frontier returns a non empty
list of pages (data available), is what we call a frontier iteration.

Current frontier iteration can be accessed using the ``iteration`` attribute.


.. _frontier-finish:

Finishing the frontier
======================

Crawl can be finished either by the Crawler or by the Crawl Frontier. Crawl frontier will finish when a maximum number
of pages are returned. This limit is controlled by the ``max_pages`` attribute (:setting:`MAX_PAGES` setting).

If ``max_pages`` has a value of 0 (default value) the frontier will continue indefinitely.

Once the frontier is finished, no more pages will be returned by the ``get_next_pages`` method and ``finished``
attribute will be True.

.. _frontier-test-mode:

Component objects
=================

.. class:: Component()

    The :class:`Component` object is the base class for frontier :class:`Middleware` and :class:`Backend` objects.

    :class:`FrontierManager` communicates with the active components using the hook methods listed below.
    Implementations are different for :class:`Middleware` and :class:`Backend` objects, therefore methods are not
    fully described here but in their corresponding section.

    Each derived class should implement the following methods:

    .. method:: frontier_start()

        Called when the frontier starts, see :ref:`Starting/Stopping the frontier <frontier-start-stop>`

    .. method:: frontier_stop()

       Called when the frontier stops, see :ref:`Starting/Stopping the frontier <frontier-start-stop>`

    .. method:: add_seeds(links)

        This method is called when new seeds are are added to the frontier

    .. method:: page_crawled(page, links)

        This method is called each time a page has been crawled

    .. method:: page_crawled_error(page, error)

        This method is called each time an error occurs when crawling a page

    .. method:: get_page(link)

        Called when a page wants to be retrieved from its URL


Test mode
=========

In some cases while testing, frontier components need to act in a different way than they usually do (for instance
domain middleware accepts non valid URLs like 'A1 or B1' when parsing domain urls in test mode).

Components can know if the frontier is in test mode via the boolean ``test_mode`` attribute.

.. _frontier-another-ways:

Another ways of using the frontier
==================================

Communication with the frontier can also be done through other mechanisms such as an HTTP API or a queue system. These
functionalities are not available for the time being, but hopefully will be included in future versions.

