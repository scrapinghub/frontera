=======
Logging
=======

Frontera is using Python native logging system. This allows a user to manage logged messages by writing a logger
configuration file (see :setting:`LOGGING_CONFIG`) or configuring logging system during runtime.

Logger configuration syntax is here
https://docs.python.org/2/library/logging.config.html

Loggers used
============

* kafka
* offset-fetcher
* cf-server
* db-worker
* strategy-worker
* messagebus.kafka
* manager.components
* manager
* frontera.contrib.backends.hbase.distributed_backend
* frontera.contrib.backends.hbase.queue
* frontera.contrib.backends.hbase.states
* frontera.contrib.backends.memory.dequequeue
* frontera.contrib.backends.memory.queue
* frontera.contrib.backends.memory.states
* frontera.contrib.backends.remote.messagebus
* frontera.contrib.backends.sqlalchemy.broad_crawling_queue
* frontera.contrib.backends.sqlalchemy.queue
* frontera.contrib.backends.sqlalchemy.revisiting.queue
* frontera.contrib.backends.sqlalchemy.states
* frontera.contrib.backends.sqlalchemy.utils
* frontera.contrib.scrapy.schedulers.FronteraScheduler

