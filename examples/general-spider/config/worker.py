# -*- coding: utf-8 -*-
from __future__ import absolute_import
from frontera.settings.default_settings import MIDDLEWARES
from config import *

MAX_NEXT_REQUESTS = 512

#--------------------------------------------------------
# Url storage
#--------------------------------------------------------

BACKEND = 'frontera.contrib.backends.sqlalchemy.Distributed'


SQLALCHEMYBACKEND_ENGINE_ECHO = False
from datetime import timedelta
SQLALCHEMYBACKEND_REVISIT_INTERVAL = timedelta(days=3)


MIDDLEWARES.extend([
    'frontera.contrib.middlewares.domain.DomainMiddleware',
    'frontera.contrib.middlewares.fingerprint.DomainFingerprintMiddleware'
])

LOGGING_CONFIG='logging.conf'


