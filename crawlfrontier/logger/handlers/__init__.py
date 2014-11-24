import sys
import logging

from crawlfrontier.logger import filters, formatters


CONSOLE = logging.StreamHandler(stream=sys.stdout)
CONSOLE.setFormatter(formatters.SHORT)

COLOR_EVENTS = logging.StreamHandler(stream=sys.stdout)
COLOR_EVENTS.setFormatter(formatters.COLORED_EVENTS)
COLOR_EVENTS.setLevel(logging.INFO)
COLOR_EVENTS.filters.append(filters.PLAINVALUES(excluded_fields=['event']))

COLOR_CONSOLE = logging.StreamHandler(stream=sys.stdout)
COLOR_CONSOLE.setFormatter(formatters.COLORED_CONSOLE)

COLOR_CONSOLE_MANAGER = logging.StreamHandler(stream=sys.stdout)
COLOR_CONSOLE_MANAGER.setFormatter(formatters.COLORED_CONSOLE_MANAGER)

COLOR_CONSOLE_BACKEND = logging.StreamHandler(stream=sys.stdout)
COLOR_CONSOLE_BACKEND.setFormatter(formatters.COLORED_CONSOLE_BACKEND)

COLOR_CONSOLE_DEBUGGING = logging.StreamHandler(stream=sys.stdout)
COLOR_CONSOLE_DEBUGGING.setFormatter(formatters.COLORED_CONSOLE_DEBUGGING)
