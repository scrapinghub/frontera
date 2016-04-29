=================
Crawling strategy
=================

Use ``frontera.worker.strategies.bfs`` module for reference. In general, you need to write a crawling strategy class
implementing the interface:

.. autoclass:: frontera.worker.strategies.BaseCrawlingStrategy

    **Methods**

    .. automethod:: frontera.worker.strategies.BaseCrawlingStrategy.from_worker
    .. automethod:: frontera.worker.strategies.BaseCrawlingStrategy.add_seeds
    .. automethod:: frontera.worker.strategies.BaseCrawlingStrategy.page_crawled
    .. automethod:: frontera.worker.strategies.BaseCrawlingStrategy.page_error
    .. automethod:: frontera.worker.strategies.BaseCrawlingStrategy.finished
    .. automethod:: frontera.worker.strategies.BaseCrawlingStrategy.close


The class can be put in any module and passed to :term:`strategy worker` using command line option or
:setting:`CRAWLING_STRATEGY` setting on startup.

The strategy class instantiated in strategy worker, and can use it's own storage or any other kind of resources. All
items from :term:`spider log` will be passed through these methods. Scores returned doesn't have to be the same as in
method arguments. Periodically ``finished()`` method is called to check if crawling goal is achieved.

