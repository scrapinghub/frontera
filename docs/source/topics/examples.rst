========
Examples
========

The project repo includes an ``examples`` folder with some scripts and projects using Frontera::

    examples/
        requests/
        general-spider/
        scrapy_recording/
        scripts/


- **requests**: Example script with `Requests`_ library.
- **general-spider**: Scrapy integration example project.
- **scrapy_recording**: Scrapy Recording example project.
- **scripts**: Some simple scripts.

.. note::

    **This examples may need to install additional libraries in order to work**.

    You can install them using pip::


        pip install -r requirements/examples.txt


requests
========

A simple script that follow all the links from a site using `Requests`_ library.

How to run it::

    python links_follower.py


general-spider
==============

A simple Scrapy spider that follows all the links from the seeds. Contains configuration files for single process,
distributed spider and backends run modes.

See :doc:`quick-start-distributed` for how to run it.


scrapy_recording
================

A simple script with a spider that follows all the links for a site, recording crawling results.

How to run it::

    scrapy crawl recorder


scripts
=======

Some sample scripts on how to use different frontier components.


.. _Requests: http://docs.python-requests.org/en/latest/