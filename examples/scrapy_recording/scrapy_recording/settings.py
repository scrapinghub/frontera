#--------------------------------------------------------------------------
# Scrapy Settings
#--------------------------------------------------------------------------
BOT_NAME = 'scrapy_frontier'

SPIDER_MODULES = ['scrapy_recording.spiders']
NEWSPIDER_MODULE = 'scrapy_recording.spiders'

HTTPCACHE_ENABLED = True
REDIRECT_ENABLED = True
COOKIES_ENABLED = False
DOWNLOAD_TIMEOUT = 20
RETRY_ENABLED = False

CONCURRENT_REQUESTS = 256
CONCURRENT_REQUESTS_PER_DOMAIN = 2

LOGSTATS_INTERVAL = 10

#DUPEFILTER_CLASS = 'scrapy.dupefilter.BaseDupeFilter'

#CLOSESPIDER_PAGECOUNT = 20

SPIDER_MIDDLEWARES = {}
DOWNLOADER_MIDDLEWARES = {
    'scrapy.contrib.downloadermiddleware.httpcache.HttpCacheMiddleware': 599,
}

#--------------------------------------------------------------------------
# Recorder Settings
#--------------------------------------------------------------------------
SPIDER_MIDDLEWARES.update({
    'crawlfrontier.contrib.scrapy.middlewares.recording.CrawlRecorderSpiderMiddleware': 0,
})
DOWNLOADER_MIDDLEWARES.update({
    'crawlfrontier.contrib.scrapy.middlewares.recording.CrawlRecorderDownloaderMiddleware': 100,  # After retry mw.
})
RECORDER_ENABLED = True
RECORDER_STORAGE_ENGINE = 'sqlite:///scrapy_recording/recordings/record.db'
RECORDER_STORAGE_DROP_ALL_TABLES = True
RECORDER_STORAGE_CLEAR_CONTENT = True

