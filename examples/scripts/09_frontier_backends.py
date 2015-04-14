"""
Test different frontier backends
"""
from frontera import FrontierManager, Settings, FrontierTester, graphs


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
    test_logic('frontera.contrib.backends.memory.FIFO')
    test_logic('frontera.contrib.backends.memory.LIFO')
    test_logic('frontera.contrib.backends.memory.BFS')
    test_logic('frontera.contrib.backends.memory.DFS')
    test_logic('frontera.contrib.backends.memory.RANDOM')
