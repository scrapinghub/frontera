class FrontierTester(object):

    def __init__(self, frontier, graph_manager, max_next_requests=0):
        self.frontier = frontier
        self.graph_manager = graph_manager
        self.max_next_requests = max_next_requests
        self.sequence = []

    def run(self, add_all_pages=False):
        if not self.frontier.auto_start:
            self.frontier.start()
        if not add_all_pages:
            self._add_seeds()
        else:
            self._add_all()
        while True:
            requests = self._run_iteration()
            self.sequence += requests
            if not requests:
                break
        self.frontier.stop()

    def _add_seeds(self):
        self.frontier.add_seeds([self._make_request(seed.url) for seed in self.graph_manager.seeds])

    def _add_all(self):
        for page in self.graph_manager.pages:
            if page.is_seed:
                self.frontier.add_seeds([self._make_request(page.url)])
            if not page.has_errors:
                for link in page.links:
                    self.frontier.add_seeds([self._make_request(link.url)])

    def _make_request(self, url):
        return self.frontier.request_model(url=url)

    def _make_response(self, url, status_code, request):
        return self.frontier.response_model(url=url, status_code=status_code, request=request)

    def _run_iteration(self):
        kwargs = {'max_next_requests': self.max_next_requests} if self.max_next_requests else {}
        requests = self.frontier.get_next_requests(**kwargs)
        for page_to_crawl in requests:
            crawled_page = self.graph_manager.get_page(url=page_to_crawl.url)
            if not crawled_page.has_errors:
                response = self._make_response(url=page_to_crawl.url,
                                               status_code=crawled_page.status,
                                               request=page_to_crawl)
                self.frontier.page_crawled(response=response,
                                           links=[self._make_request(link.url) for link in crawled_page.links])
            else:
                self.frontier.request_error(request=page_to_crawl,
                                            error=crawled_page.status)
        return requests
