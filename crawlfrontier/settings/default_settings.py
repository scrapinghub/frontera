import logging

#--------------------------------------------------------
# Frontier
#--------------------------------------------------------
PAGE_MODEL = 'crawlfrontier.core.models.Page'
LINK_MODEL = 'crawlfrontier.core.models.Link'
MIDDLEWARES = [
    'crawlfrontier.contrib.middlewares.domain.DomainMiddleware',
    'crawlfrontier.contrib.middlewares.fingerprint.UrlFingerprintMiddleware',
    'crawlfrontier.contrib.middlewares.fingerprint.DomainFingerprintMiddleware',
]
BACKEND = 'crawlfrontier.contrib.backends.memory.heapq.FIFO'
TEST_MODE = False
MAX_PAGES = 0
MAX_NEXT_PAGES = 0
AUTO_START = True

#--------------------------------------------------------
# Fingerprints mw
#--------------------------------------------------------
URL_FINGERPRINT_FUNCTION = 'crawlfrontier.utils.fingerprint.sha1'
DOMAIN_FINGERPRINT_FUNCTION = 'crawlfrontier.utils.fingerprint.sha1'

#--------------------------------------------------------
# Logging
#--------------------------------------------------------
LOGGER = 'crawlfrontier.logger.FrontierLogger'
LOGGING_ENABLED = True

LOGGING_EVENTS_ENABLED = False
LOGGING_EVENTS_INCLUDE_METADATA = True
LOGGING_EVENTS_INCLUDE_DOMAIN = True
LOGGING_EVENTS_INCLUDE_DOMAIN_FIELDS = ['name', 'netloc', 'scheme', 'sld', 'tld', 'subdomain']
LOGGING_EVENTS_HANDLERS = [
    "crawlfrontier.logger.handlers.COLOR_EVENTS",
]

LOGGING_MANAGER_ENABLED = False
LOGGING_MANAGER_LOGLEVEL = logging.DEBUG
LOGGING_MANAGER_HANDLERS = [
    "crawlfrontier.logger.handlers.COLOR_CONSOLE_MANAGER",
]

LOGGING_BACKEND_ENABLED = False
LOGGING_BACKEND_LOGLEVEL = logging.DEBUG
LOGGING_BACKEND_HANDLERS = [
    "crawlfrontier.logger.handlers.COLOR_CONSOLE_BACKEND",
]

LOGGING_DEBUGGING_ENABLED = False
LOGGING_DEBUGGING_LOGLEVEL = logging.DEBUG
LOGGING_DEBUGGING_HANDLERS = [
    "crawlfrontier.logger.handlers.COLOR_CONSOLE_DEBUGGING",
]

EVENT_LOG_MANAGER = 'crawlfrontier.logger.events.EventLogManager'
