import logging
import logging.config
from io import StringIO

colors = {
    "bold_yellow": "\x1b[01;33m",
    "green": "\x1b[32m",
    "red": "\x1b[31m",
    "reset": "\x1b[0m",
    "white": "\x1b[37m",
}

DEFAULT_LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"message": {"format": "%(message)s"}},
    "handlers": {
        "console": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": "message",
        }
    },
    "loggers": {
        "frontera": {
            "handlers": ["console"],
            "level": "DEBUG",
        },
    },
}


class LoggingCaptureMixin:
    """
    Capture the output from the 'frontera' logger and store it on the class's
    logger_output attribute.
    """

    def setUp(self):
        self.logger = logging.getLogger("frontera")
        self.old_stream = self.logger.handlers[0].stream
        self.logger_output = StringIO()
        self.logger.handlers[0].stream = self.logger_output

    def tearDown(self):
        self.logger.handlers[0].stream = self.old_stream


class SetupDefaultLoggingMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        logging.config.dictConfig(DEFAULT_LOGGING)

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
