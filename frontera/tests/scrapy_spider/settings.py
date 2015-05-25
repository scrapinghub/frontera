#--------------------------------------------------------------------------
# Scrapy Settings
#--------------------------------------------------------------------------
BOT_NAME = 'scrapy_spider'

SPIDER_MODULES = ['frontera.tests.scrapy_spider.spiders']
NEWSPIDER_MODULE = 'frontera.tests.scrapy_spider.spiders'

HTTPCACHE_ENABLED = False
REDIRECT_ENABLED = True
COOKIES_ENABLED = False
DOWNLOAD_TIMEOUT = 20
RETRY_ENABLED = False

CONCURRENT_REQUESTS = 10
CONCURRENT_REQUESTS_PER_DOMAIN = 2

LOGSTATS_INTERVAL = 10

SPIDER_MIDDLEWARES = {}
DOWNLOADER_MIDDLEWARES = {}

#--------------------------------------------------------------------------
# Frontier Settings
#--------------------------------------------------------------------------
SPIDER_MIDDLEWARES.update(
    {'frontera.contrib.scrapy.middlewares.schedulers.SchedulerSpiderMiddleware': 999},
)
DOWNLOADER_MIDDLEWARES.update(
    {'frontera.contrib.scrapy.middlewares.schedulers.SchedulerDownloaderMiddleware': 999}
)
SCHEDULER = 'frontera.contrib.scrapy.schedulers.frontier.FronteraScheduler'
FRONTERA_SETTINGS = 'frontera.tests.scrapy_spider.frontera.settings'


#--------------------------------------------------------------------------
# Testing
#--------------------------------------------------------------------------
#CLOSESPIDER_PAGECOUNT = 1
