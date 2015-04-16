================================
Using the Frontier with Requests
================================

To integrate frontier with `Requests`_ library, there is a ``RequestsFrontierManager`` class available.

This class is just a simple :class:`FrontierManager <frontera.core.manager.FrontierManager>` wrapper that uses
`Requests`_ objects (``Request``/``Response``) and converts them from and to frontier ones for you.


Use it in the same way that :class:`FrontierManager <frontera.core.manager.FrontierManager>`, initialize it with
your settings and use `Requests`_ ``Request`` and ``Response`` objects.
``get_next_requests`` method will return a `Requests`_ ``Request`` object.

An example::

    import re

    import requests

    from urlparse import urljoin

    from frontera.contrib.requests.manager import RequestsFrontierManager
    from frontera import Settings

    SETTINGS = Settings()
    SETTINGS.BACKEND = 'frontera.contrib.backends.memory.FIFO'
    SETTINGS.LOGGING_MANAGER_ENABLED = True
    SETTINGS.LOGGING_BACKEND_ENABLED = True
    SETTINGS.MAX_REQUESTS = 100
    SETTINGS.MAX_NEXT_REQUESTS = 10

    SEEDS = [
        'http://www.imdb.com',
    ]

    LINK_RE = re.compile(r'href="(.*?)"')


    def extract_page_links(response):
        return [urljoin(response.url, link) for link in LINK_RE.findall(response.text)]

    if __name__ == '__main__':

        frontier = RequestsFrontierManager(SETTINGS)
        frontier.add_seeds([requests.Request(url=url) for url in SEEDS])
        while True:
            next_requests = frontier.get_next_requests()
            if not next_requests:
                break
            for request in next_requests:
                    try:
                        response = requests.get(request.url)
                        links = [requests.Request(url=url) for url in extract_page_links(response)]
                        frontier.page_crawled(response=response, links=links)
                    except requests.RequestException, e:
                        error_code = type(e).__name__
                        frontier.request_error(request, error_code)


.. _Requests: http://docs.python-requests.org/en/latest/
