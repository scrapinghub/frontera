class FrontierTester(object):

    def __init__(self, frontier, graph_manager, max_next_pages=0):
        self.frontier = frontier
        self.graph_manager = graph_manager
        self.max_next_pages = max_next_pages
        self.sequence = []

    def run(self, add_all_pages=False):
        if not self.frontier.auto_start:
            self.frontier.start()
        if not add_all_pages:
            self._add_seeds()
        else:
            self._add_all()
        while True:
            pages = self._run_iteration()
            self.sequence += pages
            if not pages:
                break
        self.frontier.stop()

    def _add_seeds(self):
        self.frontier.add_seeds([seed.url for seed in self.graph_manager.seeds])

    def _add_all(self):
        for page in self.graph_manager.pages:
            if page.is_seed:
                self.frontier.add_seeds([page.url])
            if not page.has_errors:
                for link in page.links:
                    self.frontier.add_seeds([link.url])

    def _run_iteration(self):
        kwargs = {'max_next_pages': self.max_next_pages} if self.max_next_pages else {}
        pages_to_crawl = self.frontier.get_next_pages(**kwargs)
        for page_to_crawl in pages_to_crawl:
            crawled_page = self.graph_manager.get_page(url=page_to_crawl.url)
            if not crawled_page.has_errors:
                page_to_crawl.status = crawled_page.status
                page = self.frontier.page_crawled(page=page_to_crawl,
                                                  links=[link.url for link in crawled_page.links])
            else:
                page = self.frontier.page_crawled_error(page=page_to_crawl,
                                                        error=crawled_page.status)
        return pages_to_crawl
