from frontera.settings import Settings


class FakeFrontierManager:
    def __init__(self, settings):
        self.settings = settings
        self.auto_start = settings.get("AUTO_START")
        self.iteration = 0
        self.finished = False
        self._started = True
        self._stopped = False
        self.seeds = []
        self.requests = []
        self.links = []
        self.responses = []
        self.errors = []
        self.get_next_requests_kwargs = []

    @classmethod
    def from_settings(cls, settings=None):
        settings = Settings.object_from(settings)
        return FakeFrontierManager(settings)

    def start(self):
        self._started = True

    def stop(self):
        self._stopped = True

    def add_seeds(self, seeds):
        for seed in seeds:
            self.seeds.append(seed)

    def put_requests(self, requests):
        for request in requests:
            self.requests.append(request)

    def get_next_requests(self, max_next_requests=0, **kwargs):
        self.get_next_requests_kwargs.append(kwargs)
        max_next_requests = max_next_requests or self.settings.get("MAX_NEXT_REQUESTS")
        self.iteration += 1
        return [self.requests.pop() for _i in range(max_next_requests) if self.requests]

    def page_crawled(self, response):
        self.responses.append(response)

    def links_extracted(self, request, links):
        if links:
            for link in links:
                self.links.append(link)

    def request_error(self, request, error):
        self.errors.append((request, error))
