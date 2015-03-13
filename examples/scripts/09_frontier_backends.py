"""
Test different frontier backends
"""
from crawlfrontier import FrontierManager, Settings, FrontierTester
from crawlfrontier.utils import graphs


def test_logic(backend):
    # Graph
    graph = graphs.Manager('sqlite:///data/graph.db')

    # Frontier
    settings = Settings()
    settings.BACKEND = backend
    settings.LOGGING_MANAGER_ENABLED = True
    settings.LOGGING_BACKEND_ENABLED = True
    settings.LOGGING_DEBUGGING_ENABLED = False
    settings.TEST_MODE = True
    frontier = FrontierManager.from_settings(settings)

    # Tester
    tester = FrontierTester(frontier, graph)
    tester.run(add_all_pages=True)

    # Show crawling sequence
    print '-'*80
    print frontier.backend.name
    print '-'*80
    for page in tester.sequence:
        print page.url

if __name__ == '__main__':
    test_logic('crawlfrontier.contrib.backends.memory.FIFO')
    test_logic('crawlfrontier.contrib.backends.memory.LIFO')
    test_logic('crawlfrontier.contrib.backends.memory.BFS')
    test_logic('crawlfrontier.contrib.backends.memory.DFS')
    test_logic('crawlfrontier.contrib.backends.memory.RANDOM')
