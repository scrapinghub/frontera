========
Backends
========

Frontier :class:`Backend <frontera.core.components.Backend>` is where the crawling logic/policies lies, essentially a
brain of your crawler. :class:`Queue <frontera.core.components.Queue>`,
:class:`Metadata <frontera.core.components.Metadata>` and :class:`States <frontera.core.components.States>` are classes
where all low level code is meant to be placed, and
Backend opposite, operates on a higher levels. Frontera is bundled with database and in-memory implementations of
Queue, Metadata and States which can be combined in your custom backends or used standalone by directly
instantiating :class:`FrontierManager <frontera.core.manager.FrontierManager>` and Backend.

Backend methods are called by the FrontierManager after
:class:`Middleware <frontera.core.components.Middleware>`, using hooks for
:class:`Request <frontera.core.models.Request>` and :class:`Response <frontera.core.models.Response>` processing
according to :ref:`frontier data flow <frontier-data-flow>`.

Unlike Middleware, that can have many different instances activated, only one Backend can be used per
frontier.


.. _frontier-activating-backend:

Activating a backend
====================

To activate the frontier backend component, set it through the :setting:`BACKEND` setting.

Here’s an example::

    BACKEND = 'frontera.contrib.backends.memory.FIFO'

Keep in mind that some backends may need to be additionally configured through a particular setting. See
:ref:`backends documentation <frontier-built-in-backend>` for more info.

.. _frontier-writing-backend:

Writing your own backend
========================

Each backend component is a single Python class inherited from :class:`Backend <frontera.core.components.Backend>` or
:class:`DistributedBackend <frontera.core.components.DistributedBackend>` and using one or all of
:class:`Queue`, :class:`Metadata` and :class:`States`.

:class:`FrontierManager` will communicate with active backend through the methods described below.


.. autoclass:: frontera.core.components.Backend

    **Methods**

    .. automethod:: frontera.core.components.Backend.frontier_start

        :return: None.

    .. automethod:: frontera.core.components.Backend.frontier_stop

        :return: None.

    .. automethod:: frontera.core.components.Backend.finished

    .. automethod:: frontera.core.components.Backend.add_seeds

        :return: None.

    .. automethod:: frontera.core.components.Backend.page_crawled

        :return: None.

    .. automethod:: frontera.core.components.Backend.request_error

        :return: None.

    .. automethod:: frontera.core.components.Backend.get_next_requests

    **Class Methods**

    .. automethod:: frontera.core.components.Backend.from_manager

    **Properties**

    .. autoattribute:: frontera.core.components.Backend.queue

    .. autoattribute:: frontera.core.components.Backend.states

    .. autoattribute:: frontera.core.components.Backend.metadata


.. autoclass:: frontera.core.components.DistributedBackend

Inherits all methods of Backend, and has two more class methods, which are called during strategy and db worker
instantiation.

    .. automethod:: frontera.core.components.DistributedBackend.strategy_worker
    .. automethod:: frontera.core.components.DistributedBackend.db_worker

Backend should communicate with low-level storage by means of these classes:

Metadata
^^^^^^^^

.. autoclass:: frontera.core.components.Metadata

    **Methods**

    .. automethod:: frontera.core.components.Metadata.add_seeds

    .. automethod:: frontera.core.components.Metadata.request_error

    .. automethod:: frontera.core.components.Metadata.page_crawled


Known implementations are: :class:`MemoryMetadata` and :class:`sqlalchemy.components.Metadata`.

Queue
^^^^^

.. autoclass:: frontera.core.components.Queue

    **Methods**

    .. automethod:: frontera.core.components.Queue.get_next_requests

    .. automethod:: frontera.core.components.Queue.schedule

    .. automethod:: frontera.core.components.Queue.count

Known implementations are: :class:`MemoryQueue` and :class:`sqlalchemy.components.Queue`.

States
^^^^^^

.. autoclass:: frontera.core.components.States

    **Methods**

    .. automethod:: frontera.core.components.States.update_cache

    .. automethod:: frontera.core.components.States.set_states

    .. automethod:: frontera.core.components.States.flush

    .. automethod:: frontera.core.components.States.fetch


Known implementations are: :class:`MemoryStates` and :class:`sqlalchemy.components.States`.


.. _frontier-built-in-backend:

Built-in backend reference
==========================

This article describes all backend components that come bundled with Frontera.

To know the default activated :class:`Backend <frontera.core.components.Backend>` check the
:setting:`BACKEND` setting.


.. _frontier-backends-basic-algorithms:

Basic algorithms
^^^^^^^^^^^^^^^^
Some of the built-in :class:`Backend <frontera.core.components.Backend>` objects implement basic algorithms as
as `FIFO`_/`LIFO`_ or `DFS`_/`BFS`_ for page visit ordering.

Differences between them will be on storage engine used. For instance,
:class:`memory.FIFO <frontera.contrib.backends.memory.FIFO>` and
:class:`sqlalchemy.FIFO <frontera.contrib.backends.sqlalchemy.FIFO>` will use the same logic but with different
storage engines.

All these backend variations are using the same :class:`CommonBackend <frontera.contrib.backends.CommonBackend>` class
implementing one-time visit crawling policy with priority queue.

.. autoclass:: frontera.contrib.backends.CommonBackend


.. _frontier-backends-memory:

Memory backends
^^^^^^^^^^^^^^^

This set of :class:`Backend <frontera.core.components.Backend>` objects will use an `heapq`_ module as queue and native
dictionaries as storage for :ref:`basic algorithms <frontier-backends-basic-algorithms>`.


.. class:: frontera.contrib.backends.memory.BASE

    Base class for in-memory :class:`Backend <frontera.core.components.Backend>` objects.

.. class:: frontera.contrib.backends.memory.FIFO

    In-memory :class:`Backend <frontera.core.components.Backend>` implementation of `FIFO`_ algorithm.

.. class:: frontera.contrib.backends.memory.LIFO

    In-memory :class:`Backend <frontera.core.components.Backend>` implementation of `LIFO`_ algorithm.

.. class:: frontera.contrib.backends.memory.BFS

    In-memory :class:`Backend <frontera.core.components.Backend>` implementation of `BFS`_ algorithm.

.. class:: frontera.contrib.backends.memory.DFS

    In-memory :class:`Backend <frontera.core.components.Backend>` implementation of `DFS`_ algorithm.

.. class:: frontera.contrib.backends.memory.RANDOM

    In-memory :class:`Backend <frontera.core.components.Backend>` implementation of a random selection
    algorithm.


.. _frontier-backends-sqlalchemy:

SQLAlchemy backends
^^^^^^^^^^^^^^^^^^^

This set of :class:`Backend <frontera.core.components.Backend>` objects will use `SQLAlchemy`_ as storage for
:ref:`basic algorithms <frontier-backends-basic-algorithms>`.

By default it uses an in-memory SQLite database as a storage engine, but `any databases supported by SQLAlchemy`_ can
be used.


If you need to use your own `declarative sqlalchemy models`_, you can do it by using the
:setting:`SQLALCHEMYBACKEND_MODELS` setting.

This setting uses a dictionary where ``key`` represents the name of the model to define and ``value`` the model to use.

For a complete list of all settings used for SQLAlchemy backends check the :doc:`settings <frontera-settings>` section.

.. class:: frontera.contrib.backends.sqlalchemy.BASE

    Base class for SQLAlchemy :class:`Backend <frontera.core.components.Backend>` objects.

.. class:: frontera.contrib.backends.sqlalchemy.FIFO

    SQLAlchemy :class:`Backend <frontera.core.components.Backend>` implementation of `FIFO`_ algorithm.

.. class:: frontera.contrib.backends.sqlalchemy.LIFO

    SQLAlchemy :class:`Backend <frontera.core.components.Backend>` implementation of `LIFO`_ algorithm.

.. class:: frontera.contrib.backends.sqlalchemy.BFS

    SQLAlchemy :class:`Backend <frontera.core.components.Backend>` implementation of `BFS`_ algorithm.

.. class:: frontera.contrib.backends.sqlalchemy.DFS

    SQLAlchemy :class:`Backend <frontera.core.components.Backend>` implementation of `DFS`_ algorithm.

.. class:: frontera.contrib.backends.sqlalchemy.RANDOM

    SQLAlchemy :class:`Backend <frontera.core.components.Backend>` implementation of a random selection
    algorithm.


Revisiting backend
^^^^^^^^^^^^^^^^^^

Based on custom SQLAlchemy backend, and queue. Crawling starts with seeds. After seeds are crawled, every new
document will be scheduled for immediate crawling. On fetching every new document will be scheduled for recrawling
after fixed interval set by :setting:`SQLALCHEMYBACKEND_REVISIT_INTERVAL`.

Current implementation of revisiting backend has no prioritization. During long term runs spider could go idle, because
there are no documents available for crawling, but there are documents waiting for their scheduled revisit time.


.. class:: frontera.contrib.backends.sqlalchemy.revisiting.Backend

    Base class for SQLAlchemy :class:`Backend <frontera.core.components.Backend>` implementation of revisiting back-end.


HBase backend
^^^^^^^^^^^^^

.. autoclass:: frontera.contrib.backends.hbase.HBaseBackend

Is more suitable for large scale web crawlers. Settings reference can be found here :ref:`hbase-settings`. Consider
tunning a block cache to fit states within one block for average size website. To achieve this it's recommended to use
:attr:`hostname_local_fingerprint <frontera.utils.fingerprint.hostname_local_fingerprint>`

to achieve documents closeness within the same host. This function can be selected with :setting:`URL_FINGERPRINT_FUNCTION`
setting.

.. _FIFO: http://en.wikipedia.org/wiki/FIFO
.. _LIFO: http://en.wikipedia.org/wiki/LIFO_(computing)
.. _DFS: http://en.wikipedia.org/wiki/Depth-first_search
.. _BFS: http://en.wikipedia.org/wiki/Breadth-first_search
.. _OrderedDict: https://docs.python.org/2/library/collections.html#collections.OrderedDict
.. _heapq: https://docs.python.org/2/library/heapq.html
.. _SQLAlchemy: http://www.sqlalchemy.org/
.. _any databases supported by SQLAlchemy: http://docs.sqlalchemy.org/en/latest/dialects/index.html
.. _declarative sqlalchemy models: http://docs.sqlalchemy.org/en/latest/orm/extensions/declarative/index.html
