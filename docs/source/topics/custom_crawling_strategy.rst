================================
Writing custom crawling strategy
================================

Crawling strategy is an essential part of Frontera-based crawler and it's guiding the crawler by instructing it which pages to crawl, when and with what priority.


Crawler workflow
================

Frontera-based crawler consist of multiple processes, which are running indefinitely. The state in these processes are
persisted to a permanent storage. When processes are stopped the state is flushed and will be loaded next time when
access to certain data item is needed. Therefore it's easy to pause the crawl by stopping the processes, do the
maintenance or modify the code and start again without restarting the crawl from the beginning.

    IMPORTANT DETAIL
    Spider log (see http://frontera.readthedocs.io/en/latest/topics/glossary.html) is using hostname-based partitioning.
    The content generated from particular host will always land to the same partition (and therefore strategy worker
    instance). That guarantees the crawling strategy you design will be always dealing with same subset of hostnames
    on every SW instance. It also means the same domain cannot be operated from multiple strategy worker instances.
    To get the hostname the 2-nd level domain name is used with public suffix resolved.


To restart the crawl the

* queue contents
* link states
* domain metadata

needs to be cleaned up. This is usually done by means of truncation of tables.


Crawling strategy class
=======================

It has to be inherited from BaseCrawlingStrategy and implement it's API.

.. autoclass:: frontera.strategy.BaseCrawlingStrategy

    **Methods**

    .. automethod:: frontera.strategy.BaseCrawlingStrategy.from_worker
    .. automethod:: frontera.strategy.BaseCrawlingStrategy.read_seeds
    .. automethod:: frontera.strategy.BaseCrawlingStrategy.page_crawled
    .. automethod:: frontera.strategy.BaseCrawlingStrategy.filter_extracted_links
    .. automethod:: frontera.strategy.BaseCrawlingStrategy.links_extracted
    .. automethod:: frontera.strategy.BaseCrawlingStrategy.request_error
    .. automethod:: frontera.strategy.BaseCrawlingStrategy.finished
    .. automethod:: frontera.strategy.BaseCrawlingStrategy.close
    .. automethod:: frontera.strategy.BaseCrawlingStrategy.schedule
    .. automethod:: frontera.strategy.BaseCrawlingStrategy.create_request
    .. automethod:: frontera.strategy.BaseCrawlingStrategy.refresh_states


The class can be put in any module and passed to :term:`strategy worker` or local Scrapy process using command line
option or :setting:`CRAWLING_STRATEGY` setting on startup.

The strategy class can use its own storage or any other kind of resources. All items from :term:`spider log` will be
passed through these methods. Scores returned doesn't have to be the same as in method arguments.
Periodically ``finished()`` method is called to check if crawling goal is achieved.

Workflow
--------

There essentially two workflows: seeds addition (or injection) and main workflow. When crawl starts from scratch it
has to run the seed injection first and then proceed with main workflow. When paused/resumed crawler is running
main workflow.

Seeds addition
^^^^^^^^^^^^^^

The purpose of this step is to inject the seeds into the crawler pipeline. The framework allows to process the seeds
stream (which is read from file placed locally or in S3), create requests needed, get their link states, and schedule
them. Once requests are scheduled they will get to the queue and propagate to spiders.

To enter this workflow user is running strategy worker in add seeds mode providing arguments to crawling strategy
from command line. In particular --seeds-url is used with s3 or local file URL containing seeds to inject.

1. from_worker() → init()
1. read_seeds(stream from file, None if file isn't present)
1. exit

It's very convenient to run seeds addition using helper app in Frontera::

    $ python -m frontera.utils.add_seeds --config ... --seeds-file ...


Main
^^^^

This is the main cycle used when crawl is in progress. In a nutshell on every spider event the specific handler is
called, depending on the type of event. When strategy worker is getting the SIGTERM signal it's trying to stop politely
by calling close(). In its normal state it listens for a spider log and executes the event handlers.

1. `from_worker()` → init()
2. `page_crawled(response)` OR `page_error(request, error)` OR `filter_extracted_links(request, links)` and subsequent
   `links_extracted(request, links)`
3. `close()`
4. exit

Scheduling and creating requests
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ultimate goal of crawling strategy is scheduling of requests. To schedule request there is a method
schedule(request, score). The request is an instance of :class:`Request <frontera.core.models.Request>` class and is
often available from arguments of event handlers: _page_crawled_, _page_error_ and _links_extracted_, or can be created
on-demand using :attr:`create_request <frontera.strategy.BaseCrawlingStrategy.create_request>` method.

    IMPORTANT NOTICE

    The request created with create_request() has no state (meta[b'state']) after creation. To get the states strategy
    worker needs to access the backend, and this is not happenning when you call create_request(). Instead it is
    expected you will create a batch of requests and call refresh_states(iterable) on the whole batch of requests.
    After refresh_states is done, you will have a states available for your newly created requests.

    The Request objects created by strategy worker for event handlers are always having the states assigned.


State operations
^^^^^^^^^^^^^^^^

Every link has a state. The purpose of this states is to allow the developer to persist the state of the link in the
system (allow restart of SW components without data loss) and use it for decision making. The states are cached in
strategy worker, flushed to backend and will be loaded when needed. States are defined in
:class:`frontera.core.components.States` and can have following values:

* NOT_CRAWLED,
* QUEUED,
* CRAWLED,
* ERROR

NOT_CRAWLED is assigned when link is new, and wasn't seen previously, the rest of the state values must be assigned
in the crawling strategy code.

States allow to check that link was visited or discovered, and perform analysis of the states database to collect the
state statistics using MapReduce style jobs.


Components
==========

There are certain building blocks and successful solutions exist for the common problems.

DomainMetadata
--------------

It's often needed to persist per-host metadata in the permanent storage. To solve this there is a
:class:`frontera.core.components.DomainMetadata` instance in backend. It's has an interface of Python mapping types
(https://docs.python.org/3/library/stdtypes.html?highlight=mapping#mapping-types-dict ). It's expected that one will
be using domain names as keys and dicts as values. It's convenient to store there per-domin statistics, ban states,
the count of links found, etc.


PublicSuffix
------------
When crawling multiple domains (especially unknown ones) it's important to resolve the 2-nd level domain name properly
using publicsuffix.

Is a library from publicsuffix module provided by https://publicsuffix.org/. The purpose is to maintain a publicsuffix
of ccTLDs and name resolution routines for them in a single library. For us it's convenient to use these library
everywhere where domain name resolution is needed. Here are few examples:

* www.london.co.uk → london.co.uk
* images.yandex.ru → yandex.ru
* t.co → t.co

    As you may see the number of dots of reverted domain name cannot be used for domain name resolution.

Useful details
==============

Debugging crawling strategy
---------------------------
The best approach I found is to log all the events and outcomes using Python native logging. I.e. to setup the logger
for crawling strategy class and use it. When debug output is needed you will be able to set the logger to output to
a file, with a specific format and log level. After you have logging output set up you should start the crawl of
problematic website locally, collect and analyse the log output.

Other approaches include analysis of links database, inspecting of domain metadata and states tables, collecting the
log output of link states changes (experimental SW feature).

Meta fields
-----------

==  ==============  ===================================================================================================================================================  =========
#   name            description                                                                                                                                          presence
==  ==============  ===================================================================================================================================================  =========
1   b"slot"         Queue partitioning key in bytes, highest priority. Use it if your app requires partitioning other than default 2-nd level domain-based partitioning  Optional
2   b"domain"       Dict generated by Frontera DomainMiddleware, and containing parsed domain name                                                                       Always
3   b"state"	    Integer representing the link state, set by strategy worker. Link states are defined in frontera.core.components.States                              Always
4   b"encoding"	    In response, for HTML, encoding detected by Scrapy                                                                                                   Optional
5   b"scrapy_meta"	When scheduling can be used to set meta field for Scrapy                                                                                             Optional
==  ==============  ===================================================================================================================================================  =========

Keys and string types in nested structures are always bytes.
