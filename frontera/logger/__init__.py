import logging
import six

from frontera.utils.misc import load_object


class BaseCategoryLogger(object):

    def __init__(self, name, level=logging.DEBUG, enabled=True):
        self.name = name
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self.enabled = enabled
        for handler in self.logger.handlers:
            self.logger.removeHandler(handler)
        for filter in self.logger.filters:
            self.logger.removeFilter(filter)

    def add_handler(self, handler):
        self.logger.handlers.append(handler)

    def log(self, msg, level=logging.INFO, *args, **kwargs):
        if self.enabled:
            self.logger.log(level=level, msg=msg, *args, **kwargs)


class CategoryLogger(BaseCategoryLogger):

    def debug(self, msg):
        self.log(msg=msg, level=logging.DEBUG)

    def info(self, msg):
        self.log(msg=msg, level=logging.INFO)

    def warning(self, msg):
        self.log(msg=msg, level=logging.WARNING)

    def error(self, msg):
        self.log(msg=msg, level=logging.ERROR)

    def critical(self, msg):
        self.log(msg=msg, level=logging.CRITICAL)


class EventLogger(BaseCategoryLogger):

    def event(self, event, params):
        params['event'] = event
        self.log(msg=params, level=logging.INFO)


class FrontierLogger(object):
    def __init__(self, settings):

        logging_enabled = settings.get('LOGGING_ENABLED')

        self.events = self._start_logger(klass=EventLogger,
                                         name='events',
                                         level=logging.INFO,
                                         enabled=logging_enabled and settings.get('LOGGING_EVENTS_ENABLED'),
                                         handlers=settings.get('LOGGING_EVENTS_HANDLERS'))

        self.manager = self._start_logger(klass=CategoryLogger,
                                          name='manager',
                                          level=settings.get('LOGGING_MANAGER_LOGLEVEL'),
                                          enabled=logging_enabled and settings.get('LOGGING_MANAGER_ENABLED'),
                                          handlers=settings.get('LOGGING_MANAGER_HANDLERS'))

        self.backend = self._start_logger(klass=CategoryLogger,
                                          name='backend',
                                          level=settings.get('LOGGING_BACKEND_LOGLEVEL'),
                                          enabled=logging_enabled and settings.get('LOGGING_BACKEND_ENABLED'),
                                          handlers=settings.get('LOGGING_BACKEND_HANDLERS'))

        self.debugging = self._start_logger(klass=CategoryLogger,
                                            name='debugging',
                                            level=settings.get('LOGGING_DEBUGGING_LOGLEVEL'),
                                            enabled=logging_enabled and settings.get('LOGGING_DEBUGGING_ENABLED'),
                                            handlers=settings.get('LOGGING_DEBUGGING_HANDLERS'))

    def _start_logger(self, klass, name, level, enabled, handlers):
        logger = klass(name=name, level=level, enabled=enabled)
        for handler in handlers:
            if isinstance(handler, six.string_types):
                handler = load_object(handler)
            logger.add_handler(handler)
        return logger


def log_event(event, params, level=logging.INFO):
    logger = logging.getLogger('events')
    params['event'] = event
    logger.log(msg=params, level=level)
