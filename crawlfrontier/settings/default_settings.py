import logging

#--------------------------------------------------------
# Frontier
#--------------------------------------------------------
REQUEST_MODEL = 'crawlfrontier.core.models.Request'
RESPONSE_MODEL = 'crawlfrontier.core.models.Response'
MIDDLEWARES = [
    'crawlfrontier.contrib.middlewares.domain.DomainMiddleware',
    'crawlfrontier.contrib.middlewares.fingerprint.UrlFingerprintMiddleware',
    'crawlfrontier.contrib.middlewares.fingerprint.DomainFingerprintMiddleware',
]
BACKEND = 'crawlfrontier.contrib.backends.memory.FIFO'
TEST_MODE = False
MAX_REQUESTS = 0
MAX_NEXT_REQUESTS = 0
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
    "crawlfrontier.logger.handlers.EVENTS",
]

LOGGING_MANAGER_ENABLED = False
LOGGING_MANAGER_LOGLEVEL = logging.DEBUG
LOGGING_MANAGER_HANDLERS = [
    "crawlfrontier.logger.handlers.CONSOLE_MANAGER",
]

LOGGING_BACKEND_ENABLED = False
LOGGING_BACKEND_LOGLEVEL = logging.DEBUG
LOGGING_BACKEND_HANDLERS = [
    "crawlfrontier.logger.handlers.CONSOLE_BACKEND",
]

LOGGING_DEBUGGING_ENABLED = False
LOGGING_DEBUGGING_LOGLEVEL = logging.DEBUG
LOGGING_DEBUGGING_HANDLERS = [
    "crawlfrontier.logger.handlers.CONSOLE_DEBUGGING",
]

EVENT_LOG_MANAGER = 'crawlfrontier.logger.events.EventLogManager'
