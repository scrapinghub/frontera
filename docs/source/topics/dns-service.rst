===========
DNS Service
===========

Along with what was mentioned in :ref:`basic_requirements` you may need also a dedicated DNS Service with caching.
Especially, if your crawler is expected to generate substantial number of DNS queries. It is true for breadth-first
crawling, or any other strategies, implying accessing large number of websites, within short period of time.

Because of huge load DNS service may get blocked by your network provider eventually.

There are two options for DNS strategy:

* Recursive DNS resolution,
* using upstream servers (massive DNS caches like OpenDNS or Verizon).

The second is still prone to blocking.

There is good DNS server software https://www.unbound.net/ released by NLnet Labs. It allows to choose one of above
mentioned strategies and maintain your local DNS cache.

Have a look at Scrapy options ``REACTOR_THREADPOOL_MAXSIZE`` and ``DNS_TIMEOUT``.
