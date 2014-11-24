"""
Frontier initialization from settings
"""
from crawlfrontier import FrontierManager, Settings, graphs

SETTINGS = Settings()
SETTINGS.BACKEND = 'crawlfrontier.contrib.backends.memory.heapq.FIFO'
SETTINGS.LOGGING_MANAGER_ENABLED = True
SETTINGS.LOGGING_BACKEND_ENABLED = True
SETTINGS.LOGGING_DEBUGGING_ENABLED = True
SETTINGS.TEST_MODE = True

if __name__ == '__main__':
    # Create graph
    graph = graphs.Manager('sqlite:///data/graph.db')

    # Create frontier from settings
    frontier = FrontierManager.from_settings(SETTINGS)

    # Add seeds
    for seed in graph.seeds:
        frontier.add_seed(seed.url)

    # Get next pages
    next_pages = frontier.get_next_pages()

    # Crawl pages
    for page_to_crawl in next_pages:

        # Fake page crawling
        crawled_page = graph.get_page(page_to_crawl.url)

        # Update Page
        page_to_crawl.status = crawled_page.status
        page = frontier.page_crawled(page=page_to_crawl,
                                     links=[link.url for link in crawled_page.links])
        print repr(page)

    # Show frontier pages
    for page in frontier.backend.pages.values():
        print page.url, page.depth, page.state
