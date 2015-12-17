from datetime import timedelta

AUTO_START = True
BACKEND = 'frontera.contrib.backends.memory.FIFO'
CANONICAL_SOLVER = 'frontera.contrib.canonicalsolvers.Basic'
DELAY_ON_EMPTY = 5.0
DOMAIN_FINGERPRINT_FUNCTION = 'frontera.utils.fingerprint.sha1'
EVENT_LOG_MANAGER = 'frontera.logger.events.EventLogManager'
MAX_NEXT_REQUESTS = 64
MAX_REQUESTS = 0
MIDDLEWARES = [
    'frontera.contrib.middlewares.fingerprint.UrlFingerprintMiddleware',
]
OVERUSED_SLOT_FACTOR = 5.0
REQUEST_MODEL = 'frontera.core.models.Request'
RESPONSE_MODEL = 'frontera.core.models.Response'

SPIDER_FEED_PARTITIONS = 1
SQLALCHEMYBACKEND_CACHE_SIZE = 10000
SQLALCHEMYBACKEND_CLEAR_CONTENT = True
SQLALCHEMYBACKEND_DROP_ALL_TABLES = True
SQLALCHEMYBACKEND_ENGINE = 'sqlite:///:memory:'
SQLALCHEMYBACKEND_ENGINE_ECHO = False
SQLALCHEMYBACKEND_MODELS = {
    'MetadataModel': 'frontera.contrib.backends.sqlalchemy.models.MetadataModel',
    'StateModel': 'frontera.contrib.backends.sqlalchemy.models.StateModel',
    'QueueModel': 'frontera.contrib.backends.sqlalchemy.models.QueueModel'
}
SQLALCHEMYBACKEND_REVISIT_INTERVAL = timedelta(days=1)
STATE_CACHE_SIZE = 1000000
TEST_MODE = False
TLDEXTRACT_DOMAIN_INFO = False
URL_FINGERPRINT_FUNCTION = 'frontera.utils.fingerprint.sha1'

#--------------------------------------------------------
# Logging
#--------------------------------------------------------
LOGGER = 'frontera.logger.FrontierLogger'
LOGGING_ENABLED = True

LOGGING_EVENTS_ENABLED = False
LOGGING_EVENTS_INCLUDE_METADATA = True
LOGGING_EVENTS_INCLUDE_DOMAIN = True
LOGGING_EVENTS_INCLUDE_DOMAIN_FIELDS = ['name', 'netloc', 'scheme', 'sld', 'tld', 'subdomain']
LOGGING_EVENTS_HANDLERS = [
    "frontera.logger.handlers.EVENTS",
]

import logging

LOGGING_MANAGER_ENABLED = False
LOGGING_MANAGER_LOGLEVEL = logging.DEBUG
LOGGING_MANAGER_HANDLERS = [
    "frontera.logger.handlers.CONSOLE_MANAGER",
]

LOGGING_BACKEND_ENABLED = False
LOGGING_BACKEND_LOGLEVEL = logging.DEBUG
LOGGING_BACKEND_HANDLERS = [
    "frontera.logger.handlers.CONSOLE_BACKEND",
]

LOGGING_DEBUGGING_ENABLED = False
LOGGING_DEBUGGING_LOGLEVEL = logging.DEBUG
LOGGING_DEBUGGING_HANDLERS = [
    "frontera.logger.handlers.CONSOLE_DEBUGGING",
]


