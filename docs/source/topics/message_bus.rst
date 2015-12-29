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


