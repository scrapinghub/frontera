===============================
Fine tuning of Frontera cluster
===============================


Why crawling speed is so low?
=============================
Search for a bottleneck.

* All requests are targeted towards a few websites.
* DNS resolution (see :doc:`DNS Service <dns-service>` article),
* :term:`strategy worker` performance,
* :term:`db worker` batch generation insufficiency.
* HBase response times are too high,
* Network within cluster is overloaded.

Tuning HBase
============
* Increase block cache in HBase.
* Put Thrift server on each HBase region server and spread load from SW to Thrift.
* Enable Snappy compression.

Tuning Kafka
============
* Decrease the log size to minimum and optimize the system to avoid storing in Kafka huge volumes of data. Once data
  was written it should be consumed as fast as possible.
* Use SSD or even RAM storage for Kafka logs,
* Enable Snappy compression for Kafka.


Flow control between various components
=======================================

The :setting:`MAX_NEXT_REQUESTS` is used for controlling the batch size. In spiders config it controls how much items
will be consumed per one :attr:`get_next_requests <frontera.core.manager.FrontierManager.get_next_requests>` call. At
the same time in DB worker config it sets count of items to generate per partition. When setting these parameters keep
in mind:

* DB worker and spider values have to be consistent to avoid overloading of message bus and loosing messages. In other
  words, DB worker have to produce slightly more than consumed by spiders, because the spider should still be able to
  fetch new pages even though the DB worker has not pushed a new batch yet.
* Spider consumption rate is depending on many factors: internet connection latency, amount of spider
  parsing/scraping work, delays and auto throttling settings, usage of proxies, etc.
* Keep spider queue always full to prevent spider idling.
* General recommendation is to set DB worker value 2-4 times bigger than spiders.
* Batch size shouldn't be big to not generate too much load on backend, and allow system quickly react on queue changes.
* Watch out warnings about lost messages.

