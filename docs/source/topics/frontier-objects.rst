================
Frontier objects
================

Internally the frontier uses 2 object types: :class:`Link` and :class:`Page`.
They are used to represent the info required to manage links and pages respectively.

These classes are used by most Frontier API methods either as a parameter or as a return value depending on the method
used.

Frontier also uses these objects to internally communicate between different components (middlewares and backend).

In most cases these objects are supposed to flow around the entire system, for instance get_next_pages method returns a
list of :class:`Page` objects that must be used to call back the page_crawled method once the page has been crawled.
Scrapy Crawl Frontier middlewares automatically manages this by storing :class:`Page` objects into the
`Scrapy Request meta field`_.

Link objects
============

Used by the Frontier to manage links that represents seeds and hyperlinks extracted from crawled pages.

.. class:: Link(url)

    A :class:`Link` object represents a page hyperlink, which is usually generated in the Frontier Manager.

    :param url: the URL of the link
    :type url: string

    .. attribute:: url

        A string containing the URL of this link.

    :class:`Link` fields can be checked using the ``repr()`` function::

        >>> repr(link)
        {
            "_": "<Link:0x1034c0250:http//www.scrapinghub.com:8080>",
            "domain": {
                "_": "<Domain:0x1034c0550:?>",
                "fingerprint": "5bab61eb53176449e25c2c82f172b82cb13ffb9d",
                "name": "?",
                "netloc": "?",
                "scheme": "-",
                "sld": "-",
                "subdomain": "-",
                "tld": "-"
            },
            "fingerprint": "60d846bc2969e9706829d5f1690f11dafb70ed18",
            "url": "http//www.scrapinghub.com:8080"
        }

    Fields ``domain`` and ``fingerprint`` are added by :ref:`built-in middlewares <frontier-built-in-middleware>`


Page objects
============

.. class:: Page(url)

    A :class:`Page`  object represents a page, which is usually generated in the Frontier Manager.

    :param url: the URL of the page
    :type url: string

    .. attribute:: url

        A string containing the URL of the page.

    .. attribute:: state

        A string value (Page.State) representing the last page state. Can be one of the following values:

        - **NOT_CRAWLED**: The page has not been crawled yet.
        - **QUEUED**: Page has been returned by get_next_pages method to be crawled.
        - **CRAWLED**: Page has been crawled.
        - **ERROR**: Something went wrong while crawling the page.

    .. attribute:: depth

        The minimum depth of the page starting from a seed (depth=0).

    .. attribute:: created_at

        Page creation timestamp (in UTC).

    .. attribute:: last_update

        Page last update timestamp (in UTC).

    .. attribute:: status

        Last http status or error code for this page.

    .. attribute:: n_adds

        Counter of the number of times the page has been added.

    .. attribute:: n_queued

        Counter of the number of times the page has been queued for crawling.

    .. attribute:: n_crawls

        Counter of the number of times the page has been crawled.

    .. attribute:: n_errors

        Counter of the number of times the page generated an error while crawling.

    .. attribute:: meta

        A dict that contains arbitrary metadata for this page. This dict is empty for new Pages, and is usually
        populated by different Crawl Frontier components (middlewares, frontier, etc). So the data contained in this
        dict depends on the components you have enabled.

    .. method:: is_seed

        Return a boolean value representing if the page is a seed.

    :class:`Link` fields can be checked using the ``repr()`` function::

        >>> repr(page)
        {
            "_": "<Page:0x1034c2890:http//www.scrapinghub.com:8080>",
            "created_at": "2014-11-23T02:57:56.785935",
            "depth": 0,
            "domain": {
                "_": "<Domain:0x1034c26d0:?>",
                "fingerprint": "5bab61eb53176449e25c2c82f172b82cb13ffb9d",
                "name": "?",
                "netloc": "?",
                "scheme": "-",
                "sld": "-",
                "subdomain": "-",
                "tld": "-"
            },
            "fingerprint": "60d846bc2969e9706829d5f1690f11dafb70ed18",
            "last_update": "2014-11-23T02:57:56.788925",
            "meta": {},
            "n_adds": 1,
            "n_crawls": 0,
            "n_errors": 1,
            "n_queued": 1,
            "state": "E",
            "status": null,
            "url": "http//www.scrapinghub.com:8080"
        }

    Fields ``domain`` and ``fingerprint`` are added by :ref:`built-in middlewares <frontier-built-in-middleware>`


Creating your own frontier objects
==================================


In most cases frontier objects can be used to represent the information needed to manage the frontier logic/policy.

Also, additional page data can be stored using the :class:`Page` meta field.

.. note:: If you need to store additional data fields for page objects we recommend using :class:`Page` meta field. However your design may require adding custom fields or funcionality for your objects, the following example explains how to do this.

Let's say for instance that we want to work with a page object that needs 3 different score values (a, b and c) for
pages in order to calculate a crawl ordering priority.

We can define our own Page object inherited from :class:`Page`::

    from crawlfrontier import Page

    class ScorePage(Page):
        def __init__(self, url):
            super(ScorePage, self).__init__(url)
            self.score_a = 0
            self.score_b = 0
            self.score_c = 0

And set the frontier to use our ScorePage class using the :setting:`PAGE_MODEL` setting::

    PAGE_MODEL = 'ScorePage'

Now every page object created and used by the frontier will be a ScorePage instance.

The same case applies to :class:`Link` objects

.. _frontier-unique-objects:

Identifying unique objects
=========================

As frontier objects are shared between the crawler and the frontier, some mechanism to uniquely identify objects is
needed. This method may vary depending on the frontier logic (in most cases due to the backend used).

By default, Crawl Frontier uses the :ref:`fingerprint middleware <frontier-url-fingerprint-middleware>` to generate a
unique fingerprint calculated from the :class:`Link` or :class:`Page` url, which is added to the object. You can use
this middleware or implement your own method to manage frontier objects identification.



Nesting frontier objects
========================

Sometimes you need to create more complex fields to store object data, these fields should be a subclass of
:class:`Page` object. This needs to be that way because internally frontier may need to copy objects sometimes.

For instance the frontier :ref:`domain middleware <frontier-domain-middleware>` adds a Domain object for every
:class:`Link` and :class:`Page` object if is activated.

.. _Scrapy Request meta field: http://doc.scrapy.org/en/latest/topics/request-response.html#scrapy.http.Request.meta
