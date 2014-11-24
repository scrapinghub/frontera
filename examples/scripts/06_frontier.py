"""
Frontier usage example
"""
from crawlfrontier import FrontierManager, graphs

if __name__ == '__main__':
    # Create graph
    graph = graphs.Manager('sqlite:///data/graph.db')

    # Create frontier
    frontier = FrontierManager(
        frontier='crawlfrontier.core.frontier.Frontier',
        page_model='crawlfrontier.core.models.Page',
        link_model='crawlfrontier.core.models.Link',
        backend='crawlfrontier.contrib.backends.memory.heapq.FIFO',
        logger='crawlfrontier.logger.FrontierLogger',
        frontier_middlewares=[
            'crawlfrontier.contrib.middlewares.domain.DomainMiddleware',
            'crawlfrontier.contrib.middlewares.fingerprint.UrlFingerprintMiddleware',
            'crawlfrontier.contrib.middlewares.fingerprint.DomainFingerprintMiddleware',
        ],
        test_mode=True)

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
        frontier.page_crawled(page=page_to_crawl,
                              links=[link.url for link in crawled_page.links])

    # Show frontier pages
    for page in frontier.backend.pages.values():
        print page.url, page.depth, page.state
