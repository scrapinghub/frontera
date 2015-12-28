=====================
Architecture overview
=====================

This document describes the architecture of Frontera and how its components interact.

Overview
========

The following diagram shows an overview of the Frontera architecture with its components (referenced by numbers)
and an outline of the data flow that takes place inside the system. A brief description of the components is included
below with links for more detailed information about them. The data flow is also described below.

.. image:: _images/frontier_02.png
   :width: 793px
   :height: 280px

Components
==========

Fetcher
-------

The Fetcher (2) is responsible for fetching web pages from the sites (1) and feeding them to the frontier which manages
what pages should be crawled next.

Fetcher can be implemented using `Scrapy`_ or any other crawling framework/system as the framework offers a generic
frontier functionality.

Frontera API / Manager
----------------------

The main entry point to Frontera API (3) is the :class:`FrontierManager <frontera.core.manager.FrontierManager>` object.
Frontier users, in our case the Fetcher (2), will communicate with the frontier through it.

For more information see :doc:`frontier-api`.

Middlewares
-----------

Frontier middlewares (4) are specific hooks that sit between the Manager (3) and the Backend (5). These middlewares
process :class:`Request <frontera.core.models.Request>` and :class:`Response <frontera.core.models.Response>`
objects when they pass to and from the Frontier and the Backend. They provide a convenient mechanism for extending
functionality by plugging custom code. Canonical URL solver is a specific case of middleware responsible for
substituting non-canonical document URLs wiht canonical ones.

For more information see :doc:`frontier-middlewares` and :doc:`frontier-canonicalsolvers`


Backend
-------

The frontier Backend (5) is where the crawling logic/policies lies. It's responsible for receiving all the crawl info
and selecting the next pages to be crawled. Backend is meant to be operating on higher level, and
:class:`Queue <frontera.core.components.Queue>`, :class:`Metadata <frontera.core.components.Metadata>` and
:class:`States <frontera.core.components.States>` objects are responsible for low-level storage communication code.

May require, depending on the logic implemented, a persistent storage (6) to manage
:class:`Request <frontera.core.models.Request>` and :class:`Response <frontera.core.models.Response>`
objects info.

For more information see :doc:`frontier-backends`.

.. _frontier-data-flow:

Data Flow
=========

The data flow in Frontera is controlled by the Frontier Manager, all data passes through the
manager-middlewares-backend scheme and goes like this:

1. The frontier is initialized with a list of seed requests (seed URLs) as entry point for the crawl.
2. The fetcher asks for a list of requests to crawl.
3. Each url is fetched and the frontier is notified back of the crawl result as well of the extracted data the page
   contains. If anything went wrong during the crawl, the frontier is also informed of it.

Once all urls have been crawled, steps 2-3 are repeated until crawl of frontier end condition is reached.
Each loop (steps 2-3) repetition is called a :ref:`frontier iteration <frontier-iterations>`.


.. _Scrapy: http://scrapy.org/