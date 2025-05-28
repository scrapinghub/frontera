class BaseSchedulerMiddleware:
    def __init__(self, crawler):
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    @property
    def scheduler(self):
        try:
            # To be exposed as engine.scheduler as part of
            # https://github.com/scrapy/scrapy/pull/6715
            return self.crawler.engine._slot.scheduler
        except AttributeError:  # Scrapy < 2.13.0
            return self.crawler.engine.slot.scheduler


class SchedulerSpiderMiddleware(BaseSchedulerMiddleware):
    def process_spider_output(self, response, result, spider):
        return self.scheduler.process_spider_output(response, result, spider)


class SchedulerDownloaderMiddleware(BaseSchedulerMiddleware):
    def process_exception(self, request, exception, spider):
        return self.scheduler.process_exception(request, exception, spider)
