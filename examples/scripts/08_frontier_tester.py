"""
Frontier tester usage example
"""
from frontera import FrontierManager, FrontierTester, Settings, graphs

if __name__ == '__main__':
    # Graph
    graph = graphs.Manager('sqlite:///data/graph.db')

    # Frontier
    settings = Settings()
    settings.TEST_MODE = True
    settings.LOGGING_MANAGER_ENABLED = True
    settings.LOGGING_BACKEND_ENABLED = True
    settings.LOGGING_DEBUGGING_ENABLED = False
    frontier = FrontierManager.from_settings(settings)

    # Tester
    tester = FrontierTester(frontier, graph)

    # Run test
    tester.run()

    # Show crawling sequence
    for page in tester.sequence:
        print page.url
