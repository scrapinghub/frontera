================
Frontier objects
================

Frontier uses 2 object types: :class:`Request <frontera.core.models.Request>`
and :class:`Response <frontera.core.models.Response>`. They are used to represent crawling HTTP requests and
responses respectively.

These classes are used by most Frontier API methods either as a parameter or as a return value depending on the method
used.

Frontier also uses these objects to internally communicate between different components (middlewares and backend).


Request objects
===============

.. autoclass:: frontera.core.models.Request
    :members:


Response objects
================

.. autoclass:: frontera.core.models.Response
    :members:

Fields ``domain`` and ``fingerprint`` are added by :ref:`built-in middlewares <frontier-built-in-middleware>`


Identifying unique objects
==========================

As frontier objects are shared between the crawler and the frontier, some mechanism to uniquely identify objects is
needed. This method may vary depending on the frontier logic (in most cases due to the backend used).

By default, Frontera activates the :ref:`fingerprint middleware <frontier-url-fingerprint-middleware>` to
generate a unique fingerprint calculated from the :attr:`Request.url <frontera.core.models.Request.url>`
and :attr:`Response.url <frontera.core.models.Response.url>` fields, which is added to the
:attr:`Request.meta <frontera.core.models.Request.meta>` and
:attr:`Response.meta <frontera.core.models.Response.meta>` fields respectively. You can use
this middleware or implement your own method to manage frontier objects identification.

An example of a generated fingerprint for a :class:`Request <frontera.core.models.Request>` object::

    >>> request.url
    'http://thehackernews.com'

    >>> request.meta['fingerprint']
    '198d99a8b2284701d6c147174cd69a37a7dea90f'


.. _frontier-objects-additional-data:


Adding additional data to objects
=================================

In most cases frontier objects can be used to represent the information needed to manage the frontier logic/policy.

Also, additional data can be stored by components using the
:attr:`Request.meta <frontera.core.models.Request.meta>` and
:attr:`Response.meta <frontera.core.models.Response.meta>` fields.

For instance the frontier :ref:`domain middleware <frontier-domain-middleware>` adds a ``domain`` info field for every
:attr:`Request.meta <frontera.core.models.Request.meta>` and
:attr:`Response.meta <frontera.core.models.Response.meta>` if is activated::

    >>> request.url
    'http://www.scrapinghub.com'

    >>> request.meta['domain']
    {
        "name": "scrapinghub.com",
        "netloc": "www.scrapinghub.com",
        "scheme": "http",
        "sld": "scrapinghub",
        "subdomain": "www",
        "tld": "com"
    }
