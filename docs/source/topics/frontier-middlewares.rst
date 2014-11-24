===========
Middlewares
===========

Frontier :class:`Middleware` sits between :class:`FrontierManager` and :class:`Backend` objects, using hooks for
:class:`Link` and :class:`Page` processing according to :ref:`frontier data flow <frontier-data-flow>`.

It’s a light, low-level system for filtering and altering Frontier’s links and pages.

.. _frontier-activating-middleware:

Activating a middleware
=======================

To activate a :class:`Middleware` component, add it to the :setting:`MIDDLEWARES` setting, which is a list
whose values can be class paths or intances of :class:`Middleware` objects.

Here’s an example::

    MIDDLEWARES = [
        'crawlfrontier.contrib.middlewares.domain.DomainMiddleware',
    ]

Middlewares are called in the same order they've been defined in the list, to decide which order to assign to your
middleware pick a value according to where you want to insert it. The order does matter because each middleware
performs a different action and your middleware could depend on some previous (or subsequent) middleware being applied.

Finally, keep in mind that some middlewares may need to be enabled through a particular setting. See
:ref:`each middleware documentation <frontier-built-in-middleware>` for more info.

.. _frontier-writting-middleware:

Writing your own middleware
===========================


Writing your own frontier middleware is easy. Each :class:`Middleware` component is a single Python class inherited from
:class:`Component`.

:class:`FrontierManager` will communicate with all active middlewares through the methods described below.

.. class:: Middleware()

    It must implement the following methods:

    .. method:: frontier_start()

       Called when the frontier starts, see :ref:`Starting/Stopping the frontier <frontier-start-stop>`

       :returns: None

    .. method:: frontier_stop()

       Called when the frontier stops, see :ref:`Starting/Stopping the frontier <frontier-start-stop>`

       :returns: None

    .. method:: add_seeds(links)

        This method is called when new seeds are are added to the frontier. It receives a list of :class:`Link` objects
        created from the inital URLs passed to the :class:`FrontierManager`.

        ``add_seeds()`` should either return None or a list of :class:`Link` objects.

        If it returns None, :class:`FrontierManager` won't continue processing any other middleware and seed will never
        reach the :class:`Backend`.

        If it returns a list of :class:`Link` objects, this will be passed to next middleware. This process will repeat
        for all active middlewares until result is finally passed to the :class:`Backend`. If you want to filter any
        seed, just don't include it in the returned object list.


        :param links: A list of links created from the passed URLs.
        :type links: :class:`Link` list

        :returns: :class:`Link` list or None


    .. method:: page_crawled(page, links)

        This method is called each time a page has been crawled. It will receive the :class:`Page` object and a list
        of :class:`Link` objects created from the extracted page URLs.

        ``page_crawled()`` should either return None or a :class:`Page` object.

        If it returns None, :class:`FrontierManager` won't continue processing any other middleware and
        :class:`Backend` will never be notified.

        If it returns a :class:`Page` object, this will be passed to next middleware. This process will repeat
        for all active middlewares until result is finally passed to the :class:`Backend`.

        If you want to filter a page, just return None.

        :param page: The crawled page.
        :type page: :class:`Page`

        :param links: A list of links created from the extracted page URLs.
        :type links: :class:`Link` list

        :returns: :class:`Page` or None

    .. method:: page_crawled_error(page, error)

        This method is called each time an error occurs when crawling a page. It will receive the :class:`Page` object
        and a string containing the error code.

        ``page_crawled_error()`` should either return None or a :class:`Page` object.

        If it returns None, :class:`FrontierManager` won't continue processing any other middleware and
        :class:`Backend` will never be notified.

        If it returns a :class:`Page` object, this will be passed to next middleware. This process will repeat
        for all active middlewares until result is finally passed to the :class:`Backend`.

        If you want to filter a page, just return None.

        :param page: The crawled page with error.
        :type page: :class:`Page`

        :param links: The code of the generated error.
        :type links: string

        :returns: :class:`Page` or None

    .. method:: get_page(link)

        Called when a page wants to be retrieved from its URL. It will receive the :class:`Link` object
        generated from the page URL.

        ``get_page()`` should either return None or a :class:`Link` object.

        If it returns None, :class:`FrontierManager` won't continue processing any other middleware and
        :class:`Backend` will never be notified.

        If it returns a :class:`Link` object, this will be passed to next middleware. This process will repeat
        for all active middlewares until result is finally passed to the :class:`Backend`.

        If you want to filter a link, just return None.

        :param link: The link object created from the page URL.
        :type link: :class:`Link`

        :returns: :class:`Link` or None

.. _frontier-built-in-middleware:

Built-in middleware reference
=============================

This page describes all :class:`Middleware` components that come with Crawl Frontier. For information on how to use them and
how to write your own middleware, see the :ref:`middleware usage guide. <frontier-writting-middleware>`.

For a list of the components enabled by default (and their orders) see the :setting:`MIDDLEWARES` setting.


.. _frontier-domain-middleware:

DomainMiddleware
----------------

.. class:: crawlfrontier.contrib.middlewares.domain.DomainMiddleware

    This :class:`Middleware` will add a domain info field to all :class:`Link` and :class:`Page` objects.

    ``Domain`` object will contains the following fields:

    - **netloc**: URL netloc according to `RFC 1808`_ syntax specifications
    - **name**: Domain name
    - **scheme**: URL scheme
    - **tld**: Top level domain
    - **sld**: Second level domain
    - **subdomain**: URL subdomain(s)

    An example for a :class:`Link` object::

        >>> repr(link)
        {
            "url": "http://www.scrapinghub.com:8080/this/is/an/url"
            "domain": {
                "name": "scrapinghub.com",
                "netloc": "www.scrapinghub.com",
                "scheme": "http",
                "sld": "scrapinghub",
                "subdomain": "www",
                "tld": "com"
            }
        }

    If :setting:`TEST_MODE` is active, It will accept testing URLs, parsing letter domains::

        >>> repr(link)
        {
            "url": "A1"
            "domain": {
                "name": "A",
                "netloc": "A",
                "scheme": "-",
                "sld": "-",
                "subdomain": "-",
                "tld": "-"
            }
        }

.. _frontier-url-fingerprint-middleware:

UrlFingerprintMiddleware
------------------------

.. class:: crawlfrontier.contrib.middlewares.domain.UrlFingerprintMiddleware

    This :class:`Middleware` will add a a ``fingerprint`` field to all :class:`Link` and :class:`Page` objects.

    Fingerprint will be calculated from object ``URL``, using the function defined in :setting:`URL_FINGERPRINT_FUNCTION`
    setting. You can write your own fingerprint calculation function and use by changing this setting.

    An example for a :class:`Link` object using sha1::

        >>> repr(link)
        {
            "fingerprint": "60d846bc2969e9706829d5f1690f11dafb70ed18",
            "url": "http//www.scrapinghub.com:8080"
        }

.. _frontier-domain-fingerprint-middleware:

DomainFingerprintMiddleware
---------------------------

.. class:: crawlfrontier.contrib.middlewares.domain.DomainFingerprintMiddleware

    This :class:`Middleware` will add a a ``fingerprint`` field to all :class:`Link` and :class:`Page` objects domain fields.

    Fingerprint will be calculated from domain ``name`` field, using the function defined in :setting:`DOMAIN_FINGERPRINT_FUNCTION`
    setting. You can write your own fingerprint calculation function and use by changing this setting.

    An example for a :class:`Link` object using sha1::

        >>> repr(link)
        {
            "domain": {
                "fingerprint": "5bab61eb53176449e25c2c82f172b82cb13ffb9d",
                "name": "scrapinghub.com",
                "netloc": "www.scrapinghub.com",
                "scheme": "http",
                "sld": "scrapinghub",
                "subdomain": "www",
                "tld": "com"
            },
            "url": "http//www.scrapinghub.com:8080"
        }


.. _`RFC 1808`: http://tools.ietf.org/html/rfc1808.html
