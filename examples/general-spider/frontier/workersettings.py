# -*- coding: utf-8 -*-
from frontera.settings.default_settings import MIDDLEWARES

MAX_NEXT_REQUESTS = 512
SPIDER_FEED_PARTITIONS = 2
SPIDER_LOG_PARTITIONS = 1

#--------------------------------------------------------
# Url storage
#--------------------------------------------------------

BACKEND = 'frontera.contrib.backends.sqlalchemy.SQLAlchemyBackend'
#BACKEND = 'frontera.contrib.backends.sqlalchemy.Distributed'


SQLALCHEMYBACKEND_ENGINE = 'sqlite:///url_storage_dist.sqlite'
SQLALCHEMYBACKEND_ENGINE_ECHO = False
SQLALCHEMYBACKEND_DROP_ALL_TABLES = True
SQLALCHEMYBACKEND_CLEAR_CONTENT = True
from datetime import timedelta
SQLALCHEMYBACKEND_REVISIT_INTERVAL = timedelta(days=3)


MIDDLEWARES.extend([
    'frontera.contrib.middlewares.domain.DomainMiddleware',
    'frontera.contrib.middlewares.fingerprint.DomainFingerprintMiddleware'
])

LOGGING_CONFIG='logging.conf'


