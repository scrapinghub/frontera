==================
Installation Guide
==================

The installation steps assume that you have the following requirements installed:

* `Python`_ 2.7

* `pip`_ and `setuptools`_ Python packages. Nowadays `pip`_ requires and
  installs `setuptools`_ if not installed.

You can install Frontera using pip.

To install using pip::

   pip install frontera[option1,option2,...optionN]

Options
=======
Each option installs dependencies needed for particular functionality.

* *sql* - relational database,
* *graphs* - Graph Manager,
* *logging* - color logging,
* *tldextract* - can be used with :setting:`TLDEXTRACT_DOMAIN_INFO`
* *hbase* - HBase distributed backend,
* *zeromq* - ZeroMQ message bus,
* *kafka* - Kafka message bus,
* *distributed* - workers dependencies.

.. _Python: http://www.python.org
.. _pip: http://www.pip-installer.org/en/latest/installing.html
.. _setuptools: https://pypi.python.org/pypi/setuptools
