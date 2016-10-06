=======
Logging
=======

Frontera is using Python native logging system. That means you can manage what messages are logged and where by writing
logger configuration in files (see :setting:`LOGGING_CONFIG`).

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

