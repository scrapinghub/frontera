# -*- coding: utf-8 -*-
from frontera.settings.default_settings import MIDDLEWARES

MAX_NEXT_REQUESTS = 512
SPIDER_FEED_PARTITIONS = 1
SPIDER_LOG_PARTITIONS = 1
DELAY_ON_EMPTY = 5.0

MIDDLEWARES.extend([
    'frontera.contrib.middlewares.domain.DomainMiddleware',
    'frontera.contrib.middlewares.fingerprint.DomainFingerprintMiddleware'
])

#--------------------------------------------------------
# Crawl frontier backend
#--------------------------------------------------------
QUEUE_HOSTNAME_PARTITIONING = True
URL_FINGERPRINT_FUNCTION='frontera.utils.fingerprint.hostname_local_fingerprint'

#MESSAGE_BUS='frontera.contrib.messagebus.kafkabus.MessageBus'
#KAFKA_LOCATION = 'localhost:9092'
#SCORING_GROUP = 'scrapy-scoring'
#SCORING_TOPIC = 'frontier-score'