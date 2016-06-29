from scrapy.settings import Settings
from frontera.utils.misc import load_object


class FakeCrawler(object):

    class Slot(object):

        def __init__(self, active=0, concurrency=0):
            self.active = active
            self.concurrency = concurrency

    def __init__(self, settings=None):
        self.settings = settings or Settings()
        self.stats = load_object(self.settings['STATS_CLASS'])(self)
        dummy_class = type('class', (object,), {})
        downloader = dummy_class()
        downloader.slots = {}
        downloader.domain_concurrency = self.settings.get('CONCURRENT_REQUESTS_PER_DOMAIN')
        downloader.ip_concurrency = self.settings.get('CONCURRENT_REQUESTS_PER_IP')
        self.engine = dummy_class()
        self.engine.downloader = downloader
        self.engine.downloader.total_concurrency = self.settings.getint('CONCURRENT_REQUESTS')

    def set_slots(self, slotDict):
        slots = {}
        for key, slotPair in slotDict.iteritems():
            slots[key] = self.Slot(slotPair[0], slotPair[1])
        self.engine.downloader.slots = slots
