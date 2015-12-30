# -*- coding: utf-8 -*-
import logging

BACKEND = 'frontera.contrib.backends.sqlalchemy.revisiting.Backend'
SQLALCHEMYBACKEND_ENGINE = 'sqlite:///url_storage.sqlite'
SQLALCHEMYBACKEND_ENGINE_ECHO = False
SQLALCHEMYBACKEND_DROP_ALL_TABLES = False
SQLALCHEMYBACKEND_CLEAR_CONTENT = False
from datetime import timedelta
SQLALCHEMYBACKEND_REVISIT_INTERVAL = timedelta(days=3)

#--------------------------------------------------------
# Logging
#--------------------------------------------------------
LOGGING_ENABLED = True
LOGGING_EVENTS_ENABLED = False
LOGGING_MANAGER_ENABLED = True
LOGGING_BACKEND_ENABLED = True
LOGGING_DEBUGGING_ENABLED = False

LOGGING_BACKEND_LOGLEVEL = logging.DEBUG

DELAY_ON_EMPTY = 20.0
