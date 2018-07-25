from frontera.core.components import States
from frontera.strategy import BaseCrawlingStrategy


class BasicCrawlingStrategy(BaseCrawlingStrategy):
    def read_seeds(self, stream):
        for url in stream:
            url = url.strip()
            r = self.create_request(url)
            self.schedule(r)

    def filter_extracted_links(self, request, links):
        return links

    def links_extracted(self, request, links):
        for link in links:
            if link.meta[b'state'] == States.NOT_CRAWLED:
                self.schedule(link)
                link.meta[b'state'] = States.QUEUED

    def page_crawled(self, response):
        response.meta[b'state'] = States.CRAWLED

    def request_error(self, request, error):
        request.meta[b'state'] = States.ERROR