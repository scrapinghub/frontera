==================
frontera changelog
==================

0.7.2 (unreleased)
==================

-   Dropped support for Python 3.8 and lower, add support for Python 3.9 and
    higher.

-   Dependency updates:

    -   | core:
        | ``six`` is no longer a dependency
        | ``w3lib``: ``>=1.15.0`` → ``>=1.17.0``

    -   | ``kafka`` extra:
        | ``kafka-python``: ``>=1.0.0`` → ``>=1.4.3``
        | ``twisted`` (``>=20.3.0``) is now a dependency

    -   | ``sql`` extra:
        | ``cachetools``: ``>=0.4.0``
        | ``SQLAlchemy``: ``>=1.0.0`` → ``>=1.0.0,<1.4``

    -   | ``zeromq`` extra:
        | ``pyzmq``: ``>=19.0.2``

-   New extras: ``s3``, ``scrapy``.


Earlier releases
================

Find the earlier commit history `at GitHub
<https://github.com/scrapinghub/frontera/commits/0.7.x>`_.
