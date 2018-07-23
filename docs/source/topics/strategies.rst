===================
Crawling strategies
===================

Basic
=====

Location: :class:`frontera.strategy.basic.BasicCrawlingStrategy`

Designed to showcase the minimum amount of code needed to implement working :term:`crawling strategy`. It reads the seed
URLs, schedules all of them and crawls indefinitely all links that is discovered during the crawl.

Used for testing purposes too.


Breadth-first
=============

Location: :class:`frontera.strategy.depth.BreadthFirstCrawlingStrategy`

Starts with seed URLs provided and prioritizes links depending on their distance from seed page. The bigger the distance,
the lower the priority. This will cause close pages to be crawled first.


Depth-first
===========

Location: :class:`frontera.strategy.depth.DepthFirstCrawlingStrategy`

The same as breadth-first, but prioritization is opposite: the bigger the distance the higher the priority. Thus,
crawling deeper links first.


Discovery
=========

Location: :class:`frontera.strategy.discovery.Discovery`

This crawling strategy is used for crawling and discovery of websites in the Web. It respects robots.txt rules,
follows sitemap.xml and has a limit on a number of pages to crawl from every website. It will also skip the website in
case of fatal errors like connection reset or dns resolution errors. There are two settings used to configure it

* :setting:`DISCOVERY_MAX_PAGES`,
* :setting:`USER_AGENT`
