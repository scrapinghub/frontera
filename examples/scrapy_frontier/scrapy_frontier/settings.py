#--------------------------------------------------------------------------
# Scrapy Settings
#--------------------------------------------------------------------------
BOT_NAME = 'scrapy_frontier'

SPIDER_MODULES = ['scrapy_frontier.spiders']
NEWSPIDER_MODULE = 'scrapy_frontier.spiders'

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
# Frontier Settings
#--------------------------------------------------------------------------
SCHEDULER = 'crawlfrontier.contrib.scrapy.schedulers.frontier.CrawlFrontierScheduler'
FRONTIER_SETTINGS = 'scrapy_frontier.frontier.settings'


#--------------------------------------------------------------------------
# Seed loaders
#--------------------------------------------------------------------------
SPIDER_MIDDLEWARES.update({
    'crawlfrontier.contrib.scrapy.middlewares.seeds.file.FileSeedLoader': 1,
})
SEEDS_SOURCE = 'seeds.txt'

#--------------------------------------------------------------------------
# Testing
#--------------------------------------------------------------------------
#CLOSESPIDER_PAGECOUNT = 1
