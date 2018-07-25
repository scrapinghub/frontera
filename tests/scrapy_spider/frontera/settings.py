#--------------------------------------------------------
# Frontier
#--------------------------------------------------------
BACKEND = 'frontera.contrib.backends.sqlalchemy.Distributed'
SQLALCHEMYBACKEND_ENGINE = 'sqlite:///test.db'

MAX_REQUESTS = 5
MAX_NEXT_REQUESTS = 1

#--------------------------------------------------------
# Logging
#--------------------------------------------------------
LOGGING_EVENTS_ENABLED = False
LOGGING_MANAGER_ENABLED = False
LOGGING_BACKEND_ENABLED = False
LOGGING_DEBUGGING_ENABLED = False
