import logging

from text import DETAILED, SHORT

LOG_FORMAT = "[%(name)s] %(message)s"
LOG_EVENT_FORMAT = "%(asctime)s %(event)-16s %(message)s"

try:
    from color import ColorFormatter

    LOG_COLOR_FORMAT = "%(log_color)s"+LOG_FORMAT
    COLORS = {
        "DEBUG": "white",
        "INFO": "black",
        "WARNING": "yellow",
        "ERROR": "red",
        "CRITICAL": "bold_purple",
    }

    EVENTS = ColorFormatter(
        format="%(log_color)s"+LOG_EVENT_FORMAT,
        log_colors={
            "FRONTIER_START": "bold_yellow",
            "FRONTIER_STOP": "bold_yellow",
            "ADD_SEED": "cyan",
            "ADD_SEEDS": "cyan",
            "PAGE_CRAWLED": "blue",
            "PAGE_CRAWLED_ERROR": "red",
            "GET_NEXT_PAGES": "purple",
        },
        log_color_field="event")

    CONSOLE = ColorFormatter(
        format=LOG_COLOR_FORMAT,
        log_colors=COLORS.copy(),
        log_color_field="levelname")

    CONSOLE_MANAGER = ColorFormatter(
        format=LOG_COLOR_FORMAT,
        log_colors=COLORS.copy(),
        log_color_field="levelname")

    CONSOLE_BACKEND = ColorFormatter(
        format=LOG_COLOR_FORMAT,
        log_colors=COLORS.copy(),
        log_color_field="levelname")

    CONSOLE_DEBUGGING = ColorFormatter(
        format=LOG_COLOR_FORMAT,
        log_colors=COLORS.copy(),
        log_color_field="levelname")

    CONSOLE_MANAGER.log_colors['DEBUG'] = 'blue'
    CONSOLE_BACKEND.log_colors['DEBUG'] = 'green'
    CONSOLE_DEBUGGING.log_colors['DEBUG'] = 'cyan'


except ImportError:
    EVENTS = logging.Formatter(fmt=LOG_EVENT_FORMAT)
    CONSOLE = logging.Formatter(fmt=LOG_FORMAT)
    CONSOLE_MANAGER = logging.Formatter(fmt=LOG_FORMAT)
    CONSOLE_BACKEND = logging.Formatter(fmt=LOG_FORMAT)
    CONSOLE_DEBUGGING = logging.Formatter(fmt=LOG_FORMAT)
