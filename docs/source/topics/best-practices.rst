================
Best practices
================

.. _efficient-parallel-downloading:

Efficient parallel downloading
------------------------------

Typically the design of URL ordering implies fetching many URLs from the same domain. If crawling process needs to be
polite it has to preserve some delay and rate of requests. From the other side, there are downloaders which can afford
downloading many URLs (say 100) at once, in parallel. So, flooding of the URLs from the same domain leads to inefficient
waste of downloader connection pool resources.

Here is a short example. Imagine, we have a queue of 10K URLs from many different domains. Our task is to fetch it as
fast as possible. During downloading we want to be polite and limit per host RPS. At the same time we have a
prioritization which tends to group URLs from the same domain. When crawler will be requesting for batches of URLs to
fetch, it will be getting hundreds of URLs from the same host. The downloader will not be able to fetch them quickly
because of RPS limit and delay. Therefore, picking top URLs from the queue leeds us to the time waste, because
connection pool of downloader most of the time underused.

The solution is to supply Crawl Frontier backend with hostname/ip (usually, but not necessary) usage in downloader. We
have an `info` argument of method :attr:`get_next_requests <crawlfrontier.core.components.Backend.get_next_requests>`
pure python dictionary object for passing these stats, to the Crawl Frontier backend. Additional information can be
easilly added there. This dictionary is created outside of Crawl Frontier, and then passed to CF via
:class:`FrontierManagerWrapper <crawlfrontier.utils.managers.FrontierManagerWrapper>` subclass to backend.

There are also building blocks to ease the usage of `info` argument.

Overused keys
^^^^^^^^^^^^^

In Crawl Frontier they are stored in :func:`overused_keys <crawlfrontier.core.DownloaderInfo.overused_keys>` property
and used in backend for distinguishing overused entities.

.. automethod:: crawlfrontier.core.get_slot_key


Incorporating buffering
^^^^^^^^^^^^^^^^^^^^^^^

It's quite typical to buffer the requests for overused hostnames for later use. :class:`OverusedBuffer \
<crawlfrontier.core.OverusedBuffer>` class allows to add such functionality quickly.

.. autoclass:: crawlfrontier.core.OverusedBuffer
Here is the example how to incorporate request buffering in typical backend: ::

    class MemoryDFSOverusedBackend(MemoryDFSBackend):
        component_name = 'DFS Memory Backend taking into account overused slots'

        def __init__(self, manager):
            super(MemoryDFSOverusedBackend, self).__init__(manager)
            self._buffer = OverusedBuffer(super(MemoryDFSOverusedBackend, self).get_next_requests,
                                          manager.logger.manager.debug)

        def get_next_requests(self, max_n_requests, info):
            return self._buffer.get_next_requests(max_n_requests, info)

.. note:: For Scrapy users there is :class:`OverusedBufferScrapy \
          <crawlfrontier.contrib.scrapy.overusedbuffer.OverusedBufferScrapy>` version optimized with reusing Scrapy
          internal caches.