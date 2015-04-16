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

SPIDER_MIDDLEWARES = {}
DOWNLOADER_MIDDLEWARES = {}

#--------------------------------------------------------------------------
# Recorder Settings
#--------------------------------------------------------------------------
SPIDER_MIDDLEWARES.update(
    {'frontera.contrib.scrapy.middlewares.schedulers.SchedulerSpiderMiddleware': 999},
)
DOWNLOADER_MIDDLEWARES.update(
    {'frontera.contrib.scrapy.middlewares.schedulers.SchedulerDownloaderMiddleware': 999}
)
SCHEDULER = 'frontera.contrib.scrapy.schedulers.recording.RecorderScheduler'

RECORDER_ENABLED = True
RECORDER_STORAGE_ENGINE = 'sqlite:///scrapy_recording/recordings/record.db'
RECORDER_STORAGE_DROP_ALL_TABLES = True
RECORDER_STORAGE_CLEAR_CONTENT = True

