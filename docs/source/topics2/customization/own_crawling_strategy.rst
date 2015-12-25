=================
Crawling strategy
=================

Use ``distributed_frontera.worker.strategy.bfs`` module for reference. In general, you need to write a
``CrawlingStrategy`` class implementing the interface:

.. autoclass:: distributed_frontera.worker.strategy.base.BaseCrawlingStrategy

    **Methods**

    .. automethod:: distributed_frontera.worker.strategy.base.BaseCrawlingStrategy.add_seeds
    .. automethod:: distributed_frontera.worker.strategy.base.BaseCrawlingStrategy.page_crawled
    .. automethod:: distributed_frontera.worker.strategy.base.BaseCrawlingStrategy.page_error
    .. automethod:: distributed_frontera.worker.strategy.base.BaseCrawlingStrategy.finished


The class named ``CrawlingStrategy`` should put in a standalone module and passed to :term:`strategy worker` using
command line option on startup.

The strategy class instantiated in strategy worker, and can use it's own storage or any other kind of resources. All
items from :term:`spider log` will be passed through these methods. Scores returned doesn't have to be the same as in
method arguments. Periodically ``finished()`` method is called to check if crawling goal is achieved.

