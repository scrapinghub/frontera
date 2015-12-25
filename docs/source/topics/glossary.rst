========
Glossary
========


.. glossary::
    spider log
        A stream of encoded messages from spiders. Each message is product of extraction from document content. Most of
        the time it is links, scores, classification results.

    scoring log
        Contains score updating events and scheduling flag (if link needs to be scheduled for download) going from
        strategy worker to db worker.

    spider feed
        A stream of messages from :term:`db worker` to spiders containing new batches of documents to crawl.

    strategy worker
        Special type of worker, running the crawling strategy code: scoring the links, deciding if link needs to be
        scheduled (consults :term:`state cache`) and when to stop crawling. That type of worker is sharded.

    db worker
        Is responsible for communicating with storage DB, and mainly saving metadata and content along with
        retrieving new batches to download.

    state cache
        In-memory data structure containing information about state of documents, whatever they were scheduled or not.
        Periodically synchronized with persistent storage.

    message bus
        Transport layer abstraction mechanism. Provides interface for transport layer abstraction and several
        implementations.
