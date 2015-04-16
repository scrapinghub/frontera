"""
Custom backend example
"""
import random

from frontera import FrontierManager, Settings, FrontierTester, graphs
from frontera.contrib.backends.memory import MemoryBaseBackend


SITE_LIST = [
    [('http://google.com', [])],
    [('http://scrapinghub.com', [])],
    [('http://zynga.com', [])],
    [('http://microsoft.com', [])],
    [('http://apple.com', [])],
]


class AlphabeticSortBackend(MemoryBaseBackend):
    """
    Custom backend that sort pages alphabetically from url
    """
    name = 'Alphabetic domain name sort backend'

    def _compare_pages(self, first, second):
        return cmp(first.url, second.url)


class RandomSortBackend(MemoryBaseBackend):
    """
    Custom backend that sort pages randomly
    """
    name = 'Random sort backend'

    def _compare_pages(self, first, second):
        return random.choice([-1, 0, 1])


def test_backend(backend):

    # Graph
    graph = graphs.Manager()
    graph.add_site_list(SITE_LIST)

    # Frontier
    settings = Settings()
    settings.BACKEND = backend
    settings.LOGGING_MANAGER_ENABLED = True
    settings.LOGGING_BACKEND_ENABLED = True
    settings.LOGGING_DEBUGGING_ENABLED = False
    frontier = FrontierManager.from_settings(settings)

    print '-'*80
    print frontier.backend.name
    print '-'*80

    # Tester
    tester = FrontierTester(frontier, graph)
    tester.run()

    # Show crawling sequence
    for page in tester.sequence:
        print page.url


if __name__ == '__main__':
    test_backend('10_custom_backends.AlphabeticSortBackend')
    test_backend('10_custom_backends.RandomSortBackend')



