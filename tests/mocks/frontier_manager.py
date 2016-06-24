from frontera.settings import Settings


class FakeFrontierManager(object):

    def __init__(self, settings):
        self.settings = settings
        self.auto_start = settings.get('AUTO_START')
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
        max_next_requests = max_next_requests or self.settings.get('MAX_NEXT_REQUESTS')
        lst = []
        for i in range(max_next_requests):
            if self.requests:
                lst.append(self.requests.pop())
        self.iteration += 1
        return lst

    def page_crawled(self, response, links=None):
        if links:
            for link in links:
                self.links.append(link)
        self.responses.append(response)

    def request_error(self, request, error):
        self.errors.append((request, error))


