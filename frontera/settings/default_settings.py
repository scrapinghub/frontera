import logging

#--------------------------------------------------------
# Frontier
#--------------------------------------------------------
REQUEST_MODEL = 'frontera.core.models.Request'
RESPONSE_MODEL = 'frontera.core.models.Response'
MIDDLEWARES = [
    'frontera.contrib.middlewares.fingerprint.UrlFingerprintMiddleware',
]
BACKEND = 'frontera.contrib.backends.memory.FIFO'
CANONICAL_SOLVER = 'frontera.contrib.canonicalsolvers.Basic'
TEST_MODE = False
MAX_REQUESTS = 0
MAX_NEXT_REQUESTS = 0
AUTO_START = True
OVERUSED_SLOT_FACTOR = 5.0
DELAY_ON_EMPTY = 0.0

#--------------------------------------------------------
# Fingerprints mw
#--------------------------------------------------------
URL_FINGERPRINT_FUNCTION = 'frontera.utils.fingerprint.sha1'
DOMAIN_FINGERPRINT_FUNCTION = 'frontera.utils.fingerprint.sha1'

#--------------------------------------------------------
# Domain mw
#--------------------------------------------------------
TLDEXTRACT_DOMAIN_INFO = False

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

EVENT_LOG_MANAGER = 'frontera.logger.events.EventLogManager'
