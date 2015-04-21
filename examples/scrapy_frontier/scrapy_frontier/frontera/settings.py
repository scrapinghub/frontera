#--------------------------------------------------------
# Frontier
#--------------------------------------------------------
BACKEND = 'frontera.contrib.backends.memory.FIFO'
MAX_REQUESTS = 200
MAX_NEXT_REQUESTS = 10

#--------------------------------------------------------
# Logging
#--------------------------------------------------------
LOGGING_EVENTS_ENABLED = False
LOGGING_MANAGER_ENABLED = True
LOGGING_BACKEND_ENABLED = True
LOGGING_DEBUGGING_ENABLED = False

# Workaround for seeds loading
DELAY_ON_EMPTY = 0.0