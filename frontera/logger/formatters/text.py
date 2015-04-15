import logging

DETAILED = logging.Formatter(fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
SHORT = logging.Formatter(fmt="[%(name)-8s] %(levelname)7s: %(message)s")
