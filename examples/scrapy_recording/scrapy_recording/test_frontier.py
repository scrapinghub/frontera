"""
Frontier tester using recording data
"""
from frontera import FrontierManager, FrontierTester, Settings, graphs

SETTINGS = Settings()
SETTINGS.BACKEND = 'frontera.contrib.backends.memory_heapq.FIFO'
SETTINGS.LOGGING_MANAGER_ENABLED = True
SETTINGS.LOGGING_BACKEND_ENABLED = True
SETTINGS.LOGGING_DEBUGGING_ENABLED = False


if __name__ == '__main__':
    # Graph
    graph = graphs.Manager('sqlite:///recordings/scrapinghub.com.db')

    # Frontier
    frontier = FrontierManager.from_settings(SETTINGS)

    # Tester
    tester = FrontierTester(frontier, graph)

    # Run test
    tester.run()

    # Show frontier pages
    print '-'*80
    print ' Frontier pages'
    print '-'*80
    for page in frontier.backend.pages.values():
        print page.url, page.depth, page.state

    # Show crawling sequence
    print '-'*80
    print ' Crawling sequence'
    print '-'*80
    for page in tester.sequence:
        print page.url
