from scrapy.core.scheduler import Scheduler
from scrapy.utils.misc import load_object


DOWNLOADER_MIDDLEWARE = 'frontera.contrib.scrapy.middlewares.schedulers.SchedulerDownloaderMiddleware'
SPIDER_MIDDLEWARE = 'frontera.contrib.scrapy.middlewares.schedulers.SchedulerSpiderMiddleware'


class BaseFronteraScheduler(Scheduler):


    def _add_middlewares(self, crawler):
        """
        Adds crawl-frontier scrapy scheduler downloader and spider middlewares.
        Hack to avoid defining crawl-frontier scrapy middlewares in settings.
        Middleware managers (downloader+spider) has already been initialized at this moment.
        """
        self._add_middleware_to_manager(manager=crawler.engine.downloader.middleware,
                                        mw=load_object(DOWNLOADER_MIDDLEWARE).from_crawler(crawler))
        self._add_middleware_to_manager(manager=crawler.engine.scraper.spidermw,
                                        mw=load_object(SPIDER_MIDDLEWARE).from_crawler(crawler))

    def _add_middleware_to_manager(self, manager, mw):
        """
        Adds mw to already initialized middleware manager.
        Reproduces the mw add process at the end of the middleware manager mws list.
        """
        manager.middlewares = manager.middlewares + (mw,)
        manager._add_middleware(mw)

