======
F.A.Q.
======

.. _efficient-parallel-downloading:

How to download efficiently in parallel?
----------------------------------------

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

The solution is to supply Frontera backend with hostname/ip (usually, but not necessary) usage in downloader. We
have a keyword arguments in method :attr:`get_next_requests <frontera.core.components.Backend.get_next_requests>`
for passing these stats, to the Frontera backend. Information of any kind can be passed there. This arguments are
usually set outside of Frontera, and then passed to CF via
:class:`FrontierManagerWrapper <frontera.utils.managers.FrontierManagerWrapper>` subclass to backend.
