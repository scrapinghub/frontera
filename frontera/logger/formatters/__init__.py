from __future__ import absolute_import
import logging

LOG_FORMAT = "[%(name)s] %(message)s"

try:
    from .color import ColorFormatter

    LOG_COLOR_FORMAT = "%(log_color)s"+LOG_FORMAT
    COLORS = {
        "DEBUG": "white",
        "INFO": "green",
        "WARNING": "yellow",
        "ERROR": "red",
        "CRITICAL": "bold_purple",
    }

    CONSOLE = ColorFormatter(
        format=LOG_COLOR_FORMAT,
        log_colors=COLORS.copy(),
        log_color_field="levelname")
except ImportError:
    CONSOLE = logging.Formatter(fmt=LOG_FORMAT)
