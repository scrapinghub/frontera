#--------------------------------------------------------------------------
# Scrapy Settings
#--------------------------------------------------------------------------
BOT_NAME = 'scrapy_frontier'

SPIDER_MODULES = ['scrapy_frontier.spiders']
NEWSPIDER_MODULE = 'scrapy_frontier.spiders'

HTTPCACHE_ENABLED = True
REDIRECT_ENABLED = False
COOKIES_ENABLED = False
DOWNLOAD_TIMEOUT = 20
RETRY_ENABLED = False

CONCURRENT_REQUESTS = 256
CONCURRENT_REQUESTS_PER_DOMAIN = 2

LOGSTATS_INTERVAL = 10

#DUPEFILTER_CLASS = 'scrapy.dupefilter.BaseDupeFilter'

#CLOSESPIDER_PAGECOUNT = 1000

SPIDER_MIDDLEWARES = {}
DOWNLOADER_MIDDLEWARES = {
    'scrapy.contrib.downloadermiddleware.httpcache.HttpCacheMiddleware': 599,
}

#--------------------------------------------------------------------------
# Frontier Settings
#--------------------------------------------------------------------------
SPIDER_MIDDLEWARES.update({
    'crawlfrontier.contrib.scrapy.middlewares.frontier.CrawlFrontierSpiderMiddleware': 0,
})
DOWNLOADER_MIDDLEWARES.update({
    'crawlfrontier.contrib.scrapy.middlewares.frontier.CrawlFrontierDownloaderMiddleware': 100,  # After retry mw.
})
FRONTIER_ENABLED = True
FRONTIER_SETTINGS = 'scrapy_frontier.frontier.settings'
FRONTIER_SCHEDULER_INTERVAL = 0.01
FRONTIER_SCHEDULER_CONCURRENT_REQUESTS = 256
