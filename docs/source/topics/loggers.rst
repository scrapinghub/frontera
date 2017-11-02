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
* hbase.backend
* hbase.states
* hbase.queue
* sqlalchemy.revisiting.queue
* sqlalchemy.metadata
* sqlalchemy.states
* sqlalchemy.queue
* offset-fetcher
* overusedbuffer
* messagebus-backend
* cf-server
* db-worker
* strategy-worker
* messagebus.kafka
* memory.queue
* memory.dequequeue
* memory.states
* manager.components
* manager
* frontera.contrib.scrapy.schedulers.FronteraScheduler

