=================
Crawling strategy
=================

Use ``frontera.worker.strategies.bfs`` module for reference. In general, you need to write a crawling strategy class
implementing the interface:

.. autoclass:: frontera.core.components.BaseCrawlingStrategy

    **Methods**

    .. automethod:: frontera.core.components.BaseCrawlingStrategy.from_worker
    .. automethod:: frontera.core.components.BaseCrawlingStrategy.add_seeds
    .. automethod:: frontera.core.components.BaseCrawlingStrategy.page_crawled
    .. automethod:: frontera.core.components.BaseCrawlingStrategy.page_error
    .. automethod:: frontera.core.components.BaseCrawlingStrategy.finished
    .. automethod:: frontera.core.components.BaseCrawlingStrategy.close


The class can be put in any module and passed to :term:`strategy worker` using command line option or
:setting:`CRAWLING_STRATEGY` setting on startup.

The strategy class instantiated in strategy worker, and can use it's own storage or any other kind of resources. All
items from :term:`spider log` will be passed through these methods. Scores returned doesn't have to be the same as in
method arguments. Periodically ``finished()`` method is called to check if crawling goal is achieved.

