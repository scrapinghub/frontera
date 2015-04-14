"""
Frontier initialization from settings
"""
from frontera import FrontierManager, Settings, graphs, Request, Response

SETTINGS = Settings()
SETTINGS.BACKEND = 'frontera.contrib.backends.memory.FIFO'
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
    frontier.add_seeds([Request(seed.url) for seed in graph.seeds])

    # Get next requests
    next_requests = frontier.get_next_requests()

    # Crawl pages
    for request in next_requests:

        # Fake page crawling
        crawled_page = graph.get_page(request.url)

        # Create response
        response = Response(url=request.url,
                            status_code=crawled_page.status,
                            request=request)
        # Create page links
        page_links = [Request(link.url) for link in crawled_page.links]

        # Update Page
        frontier.page_crawled(response=response, links=page_links)
