"""
Frontier from parameters example
"""
from crawlfrontier import FrontierManager, Request, Response
from crawlfrontier.utils import graphs

if __name__ == '__main__':
    # Create graph
    graph = graphs.Manager('sqlite:///data/graph.db')

    # Create frontier
    frontier = FrontierManager(
        request_model='crawlfrontier.core.models.Request',
        response_model='crawlfrontier.core.models.Response',
        backend='crawlfrontier.contrib.backends.memory.FIFO',
        logger='crawlfrontier.logger.FrontierLogger',
        event_log_manager='crawlfrontier.logger.events.EventLogManager',
        middlewares=[
            'crawlfrontier.contrib.middlewares.domain.DomainMiddleware',
            'crawlfrontier.contrib.middlewares.fingerprint.UrlFingerprintMiddleware',
            'crawlfrontier.contrib.middlewares.fingerprint.DomainFingerprintMiddleware',
        ],
        test_mode=True)

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
