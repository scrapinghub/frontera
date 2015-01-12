========
Examples
========

The project repo includes an ``examples`` folder with some scripts and projects using CrawlFrontier::

    examples/
        requests/
        scrapy_frontier/
        scrapy_recording/
        scripts/


- **requests**: Example script with `Requests`_ library.
- **scrapy_frontier**: Scrapy Frontier example project.
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


scrapy_frontier
===============

A simple script with a spider that follows all the links for the sites defined in a ``seeds.txt`` file.

How to run it::

    scrapy crawl example


scrapy_recording
================

A simple script with a spider that follows all the links for a site, recording crawling results.

How to run it::

    scrapy crawl recorder


scripts
=======

Some sample scripts on how to use different frontier components.


.. _Requests: http://docs.python-requests.org/en/latest/