========
Settings
========

Distributed Frontera uses the same settings mechanism as Frontera, redefines some of the default values and introduces
new settings.

.. _distributed-frontera-settings:


Redefined default values
========================

.. setting:: DELAY_ON_EMPTY

DELAY_ON_EMPTY
--------------

Default: ``30.0``

Delay between calls to backend for new batches in Scrapy scheduler, when queue size is getting below
``CONCURRENT_REQUESTS``.

.. setting:: OVERUSED_SLOT_FACTOR

OVERUSED_SLOT_FACTOR
--------------------

Default: ``2.0``

(in progress + queued requests in that slot) / max allowed concurrent downloads per slot before slot is considered
overused. This affects only Scrapy scheduler.


.. setting:: URL_FINGERPRINT_FUNCTION

URL_FINGERPRINT_FUNCTION
------------------------

Default: ``frontera.utils.fingerprint.hostname_local_fingerprint``

This function is used for URL fingerprinting, which serves to uniquely identify the document in storage.
``hostname_local_fingerprint`` is constructing fingerprint getting first 4 bytes as Crc32 from host, and rest is MD5
from rest of the URL. Default option is set to make use of HBase block cache. It is expected to fit all the documents
of average website within one cache block, which can be efficiently read from disk once.


Built-in settings
=================

Here’s a list of all available Distributed Frontera settings, in alphabetical order, along with their default values
and the scope where they apply.

.. setting:: CONSUMER_BATCH_SIZE

CONSUMER_BATCH_SIZE
-------------------

Default: ``512``

This is a batch size used by strategy and db workers for consuming of spider log and scoring log streams. Increasing it
will cause worker to spend more time on every task, but processing more items per task, therefore leaving less time for
other tasks during some fixed time interval. Reducing it will result to running several tasks withing the same time
interval, but with less overall efficiency. Use it when your consumers too slow, or too fast.

.. setting:: MESSAGE_BUS

MESSAGE_BUS
-----------

Default: ``distributed_frontera.messagebus.zeromq.MessageBus``

Points Frontera to :term:`message bus` implementation. Defaults to ZeroMQ.

.. setting:: NEW_BATCH_DELAY

NEW_BATCH_DELAY
---------------

Default: ``30.0``

Used in DB worker, and it's a time interval between production of new batches for all partitions. If partition is busy,
it will be skipped.

.. setting:: HBASE_BATCH_SIZE

HBASE_BATCH_SIZE
----------------

Default: ``9216``

Count of accumulated PUT operations before they sent to HBase.

.. setting:: HBASE_DROP_ALL_TABLES

HBASE_DROP_ALL_TABLES
---------------------

Default: ``False``

Enables dropping and creation of new HBase tables on worker start.

.. setting:: HBASE_METADATA_TABLE

HBASE_METADATA_TABLE
--------------------

Default: ``metadata``

Name of the documents metadata table.

.. setting:: HBASE_NAMESPACE

HBASE_NAMESPACE
---------------

Default: ``crawler``

Name of HBase namespace where all crawler related tables will reside.

.. setting:: HBASE_QUEUE_TABLE

HBASE_QUEUE_TABLE
-----------------

Default: ``queue``

Name of HBase priority queue table.

.. setting:: HBASE_STATE_CACHE_SIZE_LIMIT

HBASE_STATE_CACHE_SIZE_LIMIT
----------------------------

Default: ``3000000``

Number of items in the :term:`state cache` of :term:`strategy worker`, before it get's flushed to HBase and cleared.

.. setting:: HBASE_THRIFT_HOST

HBASE_THRIFT_HOST
-----------------

Default: ``localhost``

HBase Thrift server host.

.. setting:: HBASE_THRIFT_PORT

HBASE_THRIFT_PORT
-----------------

Default: ``9090``

HBase Thrift server port

.. setting:: HBASE_USE_COMPACT_PROTOCOL

HBASE_USE_COMPACT_PROTOCOL
--------------------------

Default: ``False``

Whatever workers should use Thrift compact protocol. Dramatically reduces transmission overhead, but needs to be turned
on on server too.

.. setting:: HBASE_USE_SNAPPY

HBASE_USE_SNAPPY
----------------

Default: ``False``

Whatever to compress content and metadata in HBase using Snappy. Decreases amount of disk and network IO within HBase,
lowering response times. HBase have to be properly configured to support Snappy compression.

.. setting:: STORE_CONTENT

STORE_CONTENT
-------------

Default: ``False``

Determines if content should be sent over the message bus and stored in the backend: a serious performance killer.

.. setting:: SPIDER_LOG_PARTITIONS

SPIDER_LOG_PARTITIONS
---------------------

Default: ``1``

Number of :term:`spider log` stream partitions. This affects number of required :term:`strategy worker`s,
each strategy worker assigned to it's own partition.

.. setting:: SPIDER_FEED_PARTITIONS

SPIDER_FEED_PARTITIONS
----------------------

Default: ``2``

Number of :term:`spider feed` partitions. This directly affects number of spider processes running. Every spider is
assigned to it's own partition.

.. setting:: SCORING_PARTITION_ID

SCORING_PARTITION_ID
--------------------

Used by strategy worker, and represents partition startegy worker assigned to.

.. setting:: SPIDER_PARTITION_ID

SPIDER_PARTITION_ID
-------------------

Per-spider setting, pointing spider to it's assigned partition.


ZeroMQ message bus settings
===========================

The message bus class is ``distributed_frontera.messagebus.zeromq.MessageBus``

.. setting:: ZMQ_HOSTNAME

ZMQ_HOSTNAME
------------

Default: ``127.0.0.1``

Hostname, where ZeroMQ socket should bind or connect.

.. setting:: ZMQ_BASE_PORT

ZMQ_BASE_PORT
-------------

Default: ``5550``

The base port for all ZeroMQ sockets. It uses 6 sockets overall and port starting from base with step 1. Be sure that
interval [base:base+5] is available.


Kafka message bus settings
==========================

The message bus class is ``distributed_frontera.messagebus.kafkabus.MessageBus``


.. setting:: KAFKA_GET_TIMEOUT

KAFKA_GET_TIMEOUT
-----------------

Default: ``5.0``

How much time to wait for messages from Kafka consumer.

.. setting:: KAFKA_LOCATION

KAFKA_LOCATION
--------------

Hostname and port of kafka broker, separated with :. Can be a string with hostname:port pair separated with commas(,).

.. setting:: FRONTIER_GROUP

FRONTIER_GROUP
--------------

Default: ``general``

Kafka consumer group name, used for almost everything.


.. setting:: INCOMING_TOPIC

INCOMING_TOPIC
--------------

Default: ``frontier-done``

Spider log stream topic name.


.. setting:: OUTGOING_TOPIC

OUTGOING_TOPIC
--------------

Default: ``frontier-todo``

Spider feed stream topic name.


.. setting:: SCORING_GROUP

SCORING_GROUP
-------------

Default: ``strategy-workers``

A group used by strategy workers for spider log reading. Needs to be different than ``FRONTIER_GROUP``.

.. setting:: SCORING_TOPIC

SCORING_TOPIC
-------------

Kafka topic used for :term:`scoring log` stream.



