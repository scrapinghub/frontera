import sys
import logging

from frontera.logger import filters, formatters


EVENTS = logging.StreamHandler(stream=sys.stdout)
EVENTS.setFormatter(formatters.EVENTS)
EVENTS.setLevel(logging.INFO)
EVENTS.filters.append(filters.PLAINVALUES(excluded_fields=['event']))

CONSOLE = logging.StreamHandler(stream=sys.stdout)
CONSOLE.setFormatter(formatters.CONSOLE)

CONSOLE_MANAGER = logging.StreamHandler(stream=sys.stdout)
CONSOLE_MANAGER.setFormatter(formatters.CONSOLE_MANAGER)

CONSOLE_BACKEND = logging.StreamHandler(stream=sys.stdout)
CONSOLE_BACKEND.setFormatter(formatters.CONSOLE_BACKEND)

CONSOLE_DEBUGGING = logging.StreamHandler(stream=sys.stdout)
CONSOLE_DEBUGGING.setFormatter(formatters.CONSOLE_DEBUGGING)
