from __future__ import absolute_import
import sys
import logging

from frontera.logger import formatters

CONSOLE = logging.StreamHandler(stream=sys.stdout)
CONSOLE.setFormatter(formatters.CONSOLE)
