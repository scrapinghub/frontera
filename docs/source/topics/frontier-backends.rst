========
Backends
========

Frontier :class:`Backend` is where the crawling logic/policies lies. It’s responsible for receiving all the crawl info
and selecting the next pages to be crawled. It's called by the :class:`FrontierManager` after Frontier :class:`Middleware`, using hooks for :class:`Link`
and :class:`Page` processing according to :ref:`frontier data flow <frontier-data-flow>`.

Unlike :class:`Middleware`, that can have many different instances activated, only one :class:`Backend` can be used per
frontier.

Some backends require, depending on the logic implemented, a persistent storage to manage :class:`Link` and
:class:`Page` objects info.


.. _frontier-activating-backend:

Activating a backend
====================

To activate the frontier middleware component, set it through the :setting:`BACKEND` setting.

Here’s an example::

    BACKEND = 'crawlfrontier.contrib.backends.memory.heapq.FIFO'

Keep in mind that some backends may need to be enabled through a particular setting. See
:ref:`each backend documentation <frontier-built-in-backend>` for more info.

.. _frontier-writting-backend:

Writing your own backend
===========================

Writing your own frontier backend is easy. Each :class:`Backend` component is a single Python class inherited from
:class:`Component`.

:class:`FrontierManager` will communicate with active :class:`Backend` through the methods described below.

.. class:: Backend()

    It must implement the following methods:

    .. method:: frontier_start()

       Called when the frontier starts, see :ref:`Starting/Stopping the frontier <frontier-start-stop>`

       :returns: None

    .. method:: frontier_stop()

       Called when the frontier stops, see :ref:`Starting/Stopping the frontier <frontier-start-stop>`

       :returns: None

    .. method:: add_seeds(links)

        This method is called when new seeds are are added to the frontier. It receives a list of :class:`Link` objects
        created from the inital URLs passed to the :class:`FrontierManager`.

        ``add_seeds()`` should either return None or a list of :class:`Page` objects.

        .. note:: :class:`Backend` must create and return the different :class:`Page` objects created from the passed :class:`Link` objects list.

        :param links: A list of links created from the passed URLs.
        :type links: :class:`Link` list

        :returns: :class:`Page` list or None


    .. method:: page_crawled(page, links)

        This method is called each time a page has been crawled. It will receive the :class:`Page` object and a list
        of :class:`Link` objects created from the extracted page URLs.

        ``page_crawled()`` should either return None or a :class:`Page` object.

        :param page: The crawled page.
        :type page: :class:`Page`

        :param links: A list of links created from the extracted page URLs.
        :type links: :class:`Link` list

        :returns: :class:`Page` or None

    .. method:: page_crawled_error(page, error)

        This method is called each time an error occurs when crawling a page. It will receive the :class:`Page` object
        and a string containing the error code.

        ``page_crawled_error()`` should either return None or a :class:`Page` object.

        :param page: The crawled page with error.
        :type page: :class:`Page`

        :param links: The code of the generated error.
        :type links: string

        :returns: :class:`Page` or None

    .. method:: get_page(link)

        Called when a page wants to be retrieved from its URL. It will receive the :class:`Link` object
        generated from the page URL.

        ``get_page()`` should either return None or a :class:`Page` object.

        .. note:: If page exists :class:`Backend` must return a :class:`Page` object created from the passed :class:`Link`.

        :param link: The link object created from the page URL.
        :type link: :class:`Link`

        :returns: :class:`Page` or None


.. _frontier-built-in-backend:

Built-in backend reference
=============================

This page describes all :class:`Backend` components that come with Crawl Frontier. For information on how to use them and
how to write your own middleware, see the :ref:`backend usage guide. <frontier-writting-backend>`.

To know the default activated :class:`Backend` check the :setting:`BACKEND` setting.

.. _frontier-backends-basic-algorithms:

basic algorithms
----------------
Some of the built-in :class:`Backend` objects implement basic algorithms as as `FIFO`_/`LIFO`_ or
`DFS`_/`BFS`_ for page visit ordering.

Differences between them will be on storage engine used. For instance, ``memory.FIFO``, ``heapq.FIFO`` and
``sqlalchemy.FIFO`` will use the same logic but with different storage engines.

.. _frontier-backends-memory:

memory.dict backends
--------------------

This set of :class:`Backend` objects will use an `OrderedDict`_ python object as storage for
:ref:`basic algorithms <frontier-backends-basic-algorithms>`.

.. warning:: Use of `OrderedDict`_ for large crawls is **extremely inefficient in terms of speed**, if you want to use a volatile storage engine use :ref:`heapq implementations <frontier-backends-heapq>` instead. This storages have been intentionally left in the framework for learning purposes.

.. class:: crawlfrontier.contrib.backends.memory.dict.BASE

    Base class for in-memory dict :class:`Backend` objects.

.. class:: crawlfrontier.contrib.backends.memory.dict.FIFO

    In-memory dict :class:`Backend` implementation of `FIFO`_ algorithm.

.. class:: crawlfrontier.contrib.backends.memory.dict.LIFO

    In-memory dict  :class:`Backend` implementation of `LIFO`_ algorithm.

.. class:: crawlfrontier.contrib.backends.memory.dict.BFS

    In-memory dict :class:`Backend` implementation of `BFS`_ algorithm.

.. class:: crawlfrontier.contrib.backends.memory.dict.DFS

    In-memory dict :class:`Backend` implementation of `DFS`_ algorithm.

.. _frontier-backends-heapq:

memory.heapq backends
---------------------

This set of :class:`Backend` objects will use an `heapq`_ object as storage for
:ref:`basic algorithms <frontier-backends-basic-algorithms>`.

.. class:: crawlfrontier.contrib.backends.memory.heapq.BASE

    Base class for in-memory heapq :class:`Backend` objects.

.. class:: crawlfrontier.contrib.backends.memory.heapq.FIFO

    In-memory heapq :class:`Backend` implementation of `FIFO`_ algorithm.

.. class:: crawlfrontier.contrib.backends.memory.heapq.LIFO

    In-memory heapq :class:`Backend` implementation of `LIFO`_ algorithm.

.. class:: crawlfrontier.contrib.backends.memory.heapq.BFS

    In-memory heapq :class:`Backend` implementation of `BFS`_ algorithm.

.. class:: crawlfrontier.contrib.backends.memory.heapq.DFS

    In-memory heapq :class:`Backend` implementation of `DFS`_ algorithm.

.. _frontier-backends-sqlalchemy:

sqlalchemy backends
---------------------

This set of :class:`Backend` objects will use `SQLAlchemy`_ as storage for
:ref:`basic algorithms <frontier-backends-basic-algorithms>`.

By default it uses memory as storage engine, but `any other SQLAlchemy supported databases`_ can be used.

:class:`Page` objects are represented by a `declarative sqlalchemy model`_::

    class PageBase(Model):
        __abstract__ = True
        __tablename__ = 'pages'
        __table_args__ = (
            UniqueConstraint('url'),
        )

        class States:
            NOT_CRAWLED = 'N'
            QUEUED = 'Q'
            CRAWLED = 'C'
            ERROR = 'E'

        STATES = [
            (States.NOT_CRAWLED, FrontierPage.State.NOT_CRAWLED),
            (States.QUEUED, FrontierPage.State.QUEUED),
            (States.CRAWLED, FrontierPage.State.CRAWLED),
            (States.ERROR, FrontierPage.State.ERROR),
        ]

        fingerprint = Column(FINGERPRINT, primary_key=True, nullable=False, index=True, unique=True)
        url = Column(URL_FIELD, nullable=False)
        depth = Column(Integer, nullable=False)
        created_at = Column(TIMESTAMP, nullable=False)
        last_update = Column(TIMESTAMP, nullable=False)
        status = Column(String(20))
        last_redirects = Column(Integer)
        last_iteration = Column(Integer, nullable=False)
        state = Column(Choice(choices=STATES, default=States.NOT_CRAWLED))
        n_adds = Column(Integer, default=0)
        n_queued = Column(Integer, default=0)
        n_crawls = Column(Integer, default=0)
        n_errors = Column(Integer, default=0)

If you need to create your own models, you can do it by using the :setting:`DEFAULT_MODELS` setting::

    DEFAULT_MODELS = {
        'Page': 'crawlfrontier.contrib.backends.sqlalchemy.models.Page',
    }

This setting uses a dictionary where ``key`` represents the name of the model to define and ``value`` the model to use.
If you want for instance to create a model to represent domains::

    DEFAULT_MODELS = {
        'Page': 'crawlfrontier.contrib.backends.sqlalchemy.models.Page',
        'Domain': 'myproject.backends.sqlalchemy.models.Domain',
    }

Models can be accessed from the :class:`Backend` dictionary attribute ``models``.

For a complete list of all settings used for sqlalchemy backends check the :doc:`settings <frontier-settings>` section.

.. class:: crawlfrontier.contrib.backends.sqlalchemy.BASE

    Base class for SQLAlchemy :class:`Backend` objects.

.. class:: crawlfrontier.contrib.backends.sqlalchemy.FIFO

    SQLAlchemy :class:`Backend` implementation of `FIFO`_ algorithm.

.. class:: crawlfrontier.contrib.backends.sqlalchemy.LIFO

    SQLAlchemy heapq :class:`Backend` implementation of `LIFO`_ algorithm.

.. class:: crawlfrontier.contrib.backends.sqlalchemy.BFS

    SQLAlchemy heapq :class:`Backend` implementation of `BFS`_ algorithm.

.. class:: crawlfrontier.contrib.backends.sqlalchemy.DFS

    SQLAlchemy heapq :class:`Backend` implementation of `DFS`_ algorithm.



.. _FIFO: http://en.wikipedia.org/wiki/FIFO
.. _LIFO: http://en.wikipedia.org/wiki/LIFO_(computing)
.. _DFS: http://en.wikipedia.org/wiki/Depth-first_search
.. _BFS: http://en.wikipedia.org/wiki/Breadth-first_search
.. _OrderedDict: https://docs.python.org/2/library/collections.html#collections.OrderedDict
.. _heapq: https://docs.python.org/2/library/heapq.html
.. _SQLAlchemy: http://www.sqlalchemy.org/
.. _any other SQLAlchemy supported databases: http://docs.sqlalchemy.org/en/rel_0_9/dialects/index.html
.. _declarative sqlalchemy model: http://docs.sqlalchemy.org/en/rel_0_9/orm/extensions/declarative.html
