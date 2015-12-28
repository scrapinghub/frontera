.. _topics-index:

================================
Frontera |version| documentation
================================

This documentation contains (almost) everything you need to know about Frontera.

Introduction
============

.. toctree::
   :hidden:

   topics/overview
   topics/run-modes


:doc:`topics/overview`
    Understand what Frontera is and how it can help you.

:doc:`topics/run-modes`
    High level architecture and Frontera run modes.


Using Frontera
==============

.. toctree::
   :hidden:

   topics/installation
   topics/frontier-objects
   topics/frontier-middlewares
   topics/frontier-canonicalsolvers
   topics/frontier-backends
   topics/own_crawling_strategy
   topics/scrapy-integration
   topics/frontera-settings

:doc:`topics/installation`
    HOWTO and Dependencies options.

:doc:`topics/frontier-objects`
    Understand the classes used to represent requests and responses.

:doc:`topics/frontier-middlewares`
    Filter or alter information for links and documents.

:doc:`topics/frontier-canonicalsolvers`
    Identify and make use of canonical url of document.

:doc:`topics/frontier-backends`
    Define your own crawling policy and custom storage.

:doc:`topics/own_crawling_strategy`
    Implementing own crawling strategy for distributed backend.

:doc:`topics/scrapy-integration`
    Learn how to use Frontera with Scrapy.

:doc:`topics/frontera-settings`
    Settings reference.


Extending Frontera
==================

.. toctree::
   :hidden:

:doc:`topics/what-is-frontera`
    Learn what Frontera is and how to use it.

:doc:`topics/architecture`
    See how Frontera works and its different components.

:doc:`topics/frontier-api`
    Learn how to use the frontier.

   topics/what-is-frontera
   topics/architecture
   topics/frontier-api


Built-in services and tools
===========================

.. toctree::
   :hidden:

   topics/requests-integration
   topics/graph-manager
   topics/frontier-tester
   topics/scrapy-recorder



:doc:`topics/requests-integration`
    Learn how to use Frontera with Requests.

:doc:`topics/graph-manager`
    Define fake crawlings for websites to test your frontier.

:doc:`topics/frontier-tester`
    Test your frontier in an easy way.

:doc:`topics/scrapy-recorder`
    Create Scrapy crawl recordings and reproduce them later.

:doc:`topics/seed-loaders`
    Scrapy middlewares for seed loading

All the rest
============

.. toctree::
   :hidden:

   topics/examples
   topics/best-practices
   topics/tests
   topics/glossary

:doc:`topics/examples`
    Some example projects and scripts using Frontera.

:doc:`topics/best-practices`
    The best practices of Frontera usage.

:doc:`topics/tests`
    How to run and write Frontera tests.

:doc:`topics/glossary`
    Glossary of terms.

..   topics/frontier-at-a-glance
   topics/installation
   topics/what-is-a-crawl-frontier
   topics/architecture
   topics/frontier-objects
   topics/frontier-api
   topics/frontier-middlewares
   topics/frontier-backends
   topics/frontera-settings
   topics/scrapy-integration
   topics/graph-manager
   topics/frontier-tester
   topics/scrapy-recorder
   topics/release-notes

..   topics/frontier-logging
   topics/seed-loaders
   topics/faq
   topics/wip
   topics/common-practices
   topics/contributing
