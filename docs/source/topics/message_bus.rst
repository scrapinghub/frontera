===========
Message bus
===========

Is the transport layer abstraction mechanism. It provides interface and several implementations. Only one message bus
can be used in crawler at the time, and it's selected with :setting:`MESSAGE_BUS` vim maksetting.

Spiders process can use

.. autoclass:: frontera.contrib.backends.remote.messagebus.MessageBusBackend

to communicate using message bus.


Built-in message bus reference
==============================

ZeroMQ
------
It's the default option, implemented using lightweight `ZeroMQ`_ library in

.. autoclass:: frontera.contrib.messagebus.zeromq.MessageBus

and can be configured using :ref:`zeromq-settings`.

ZeroMQ message bus requires installed ZeroMQ library and running broker process, see :ref:`running_zeromq_broker`.

.. pull-quote::
    WARNING! ZeroMQ message bus doesn't support yet multiple SW and DB workers, only one instance of each worker
    type is allowed.


Kafka
-----
Can be selected with

.. autoclass:: frontera.contrib.messagebus.kafkabus.MessageBus

and configured using :ref:`kafka-settings`.

Requires running `Kafka`_ service and more suitable for large-scale web crawling.

.. _Kafka: http://kafka.apache.org/
.. _ZeroMQ: http://zeromq.org/


Protocol
========

Depending on stream Frontera is using several message types to code it's messages. Every message is a python native
object serialized using `msgpack`_ (also JSON is available, but needs to be selected in code manually).

Here are the classes needed to subclass to implement own codec:

.. autoclass:: frontera.core.codec.BaseEncoder

    .. automethod:: frontera.core.codec.BaseEncoder.encode_add_seeds
    .. automethod:: frontera.core.codec.BaseEncoder.encode_page_crawled
    .. automethod:: frontera.core.codec.BaseEncoder.encode_request_error
    .. automethod:: frontera.core.codec.BaseEncoder.encode_request
    .. automethod:: frontera.core.codec.BaseEncoder.encode_update_score
    .. automethod:: frontera.core.codec.BaseEncoder.encode_new_job_id
    .. automethod:: frontera.core.codec.BaseEncoder.encode_offset

.. autoclass:: frontera.core.codec.BaseDecoder

    .. automethod:: frontera.core.codec.BaseDecoder.decode
    .. automethod:: frontera.core.codec.BaseDecoder.decode_request


.. _msgpack: http://msgpack.org/index.html