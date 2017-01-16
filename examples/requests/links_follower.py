from __future__ import print_function

import re

import requests

from frontera.contrib.requests.manager import RequestsFrontierManager
from frontera import Settings

from six.moves.urllib.parse import urljoin


SETTINGS = Settings()
SETTINGS.BACKEND = 'frontera.contrib.backends.memory.FIFO'
SETTINGS.LOGGING_MANAGER_ENABLED = True
SETTINGS.LOGGING_BACKEND_ENABLED = True
SETTINGS.MAX_REQUESTS = 100
SETTINGS.MAX_NEXT_REQUESTS = 10

SEEDS = [
    'http://www.imdb.com',
]

LINK_RE = re.compile(r'<a.+?href="(.*?)".?>', re.I)


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
                    links = [
                        requests.Request(url=url)
                        for url in extract_page_links(response)
                    ]
                    frontier.page_crawled(response)
                    print('Crawled', response.url, '(found', len(links), 'urls)')

                    if links:
                        frontier.links_extracted(request, links)
                except requests.RequestException as e:
                    error_code = type(e).__name__
                    frontier.request_error(request, error_code)
                    print('Failed to process request', request.url, 'Error:', e)
