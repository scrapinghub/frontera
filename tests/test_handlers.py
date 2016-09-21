import unittest
import logging
import logging.config
import re
import redis

from frontera.logger.handlers.redis import RedisListHandler
from frontera.logger.handlers import EVENTS, CONSOLE, CONSOLE_MANAGER, CONSOLE_BACKEND, CONSOLE_DEBUGGING
from w3lib.util import to_native_str
from tests.utils import SetupDefaultLoggingMixin, LoggingCaptureMixin, colors


class TestRedisListHandler(unittest.TestCase):

    r = None

    def setUp(self):
        if not self.r:
            self.r = redis.Redis()
        self.logger = logging.getLoggerClass()('frontera')

    def tearDown(self):
        self.r.flushall()
        del self.logger

    def initialise_handler(self, use_to=False, list_name='test_list', max_messages=0,
                           formatter=None, filters=None, level=logging.NOTSET):
        if use_to:
            self.logger.addHandler(RedisListHandler.to(list_name=list_name,
                                                       max_messages=max_messages,
                                                       level=level))
            return
        self.logger.addHandler(RedisListHandler(list_name=list_name,
                                                max_messages=max_messages,
                                                formatter=formatter,
                                                filters=filters,
                                                level=level))

    def get_logged_messages(self):
        return [to_native_str(x) for x in self.r.lrange('test_list', 0, 10)]

    def test_handler_list_name(self):
        self.assert_checks_handler_list_name()

    def test_handler_list_name_use_to(self):
        self.assert_checks_handler_list_name()

    def assert_checks_handler_list_name(self, use_to=False):
        self.initialise_handler(use_to=use_to)
        self.logger.debug('debug message')
        self.logger.info('info message')
        msgs = self.get_logged_messages()
        self.assertEqual(msgs, ['debug message', 'info message'])

    def test_handler_max_messages(self):
        self.assert_checks_handler_max_messages()

    def test_handler_max_messages_use_to(self):
        self.assert_checks_handler_max_messages(use_to=True)

    def assert_checks_handler_max_messages(self, use_to=False):
        self.initialise_handler(use_to=use_to, max_messages=2)
        self.logger.debug('debug message')
        self.logger.info('info message')
        self.logger.error('error message')
        msgs = self.get_logged_messages()
        self.assertEqual(msgs, ['info message', 'error message'])

    def test_handler_level(self):
        self.assert_checks_handler_level()

    def test_handler_level_use_to(self):
        self.assert_checks_handler_level(use_to=True)

    def assert_checks_handler_level(self, use_to=False):
        self.initialise_handler(use_to=use_to, level=logging.INFO)
        self.logger.debug('debug message')
        self.logger.info('info message')
        msgs = self.get_logged_messages()
        self.assertEqual(msgs, ['info message'])

    def test_handler_with_formatter(self):
        from frontera.logger.formatters.text import SHORT
        self.initialise_handler(formatter=SHORT)
        self.logger.debug('debug message')
        msgs = self.get_logged_messages()
        self.assertEqual(msgs, ['[frontera]   DEBUG: debug message'])

    def test_handler_with_filter(self):
        class ListJoinFilter(logging.Filter):
            def filter(self, record):
                if isinstance(record.msg, list):
                    record.msg = ', '.join(record.msg)
                return True
        self.initialise_handler(filters=[ListJoinFilter()])
        self.logger.debug(['debug message', 'error message'])
        msgs = self.get_logged_messages()
        self.assertEqual(msgs, ['debug message, error message'])


class SetupHandler(SetupDefaultLoggingMixin):
    @classmethod
    def setUpClass(cls):
        super(SetupHandler, cls).setUpClass()
        l = logging.getLogger('frontera')
        l.handlers[0] = cls.handler


class BaseTestHandlers(SetupHandler, LoggingCaptureMixin, unittest.TestCase):
    handler = None


class TestHandlerEvent(BaseTestHandlers):
    handler = EVENTS

    def test_handler_events_formatter(self):
        self.logger.info('info message', extra={'event': 'FRONTIER_START'})
        self.assertRegexpMatches(self.logger_output.getvalue(),
                                 '{bold_yellow}\d{{4}}-\d{{2}}-\d{{2}} \d{{2}}:\d{{2}}:\d{{2}},\d+ '
                                 'FRONTIER_START   info message{reset}\n'.format(
                                     bold_yellow=re.escape(colors['bold_yellow']),
                                     reset=re.escape(colors['reset'])))

    def test_handler_events_level(self):
        self.logger.debug('debug message', extra={'event': 'FRONTIER_START'})
        self.logger.info('info message', extra={'event': 'FRONTIER_STOP'})
        self.assertRegexpMatches(self.logger_output.getvalue(),
                                 '{bold_yellow}\d{{4}}-\d{{2}}-\d{{2}} \d{{2}}:\d{{2}}:\d{{2}},\d+ '
                                 'FRONTIER_STOP    info message{reset}\n'.format(
                                     bold_yellow=re.escape(colors['bold_yellow']),
                                     reset=re.escape(colors['reset'])))

    def test_handler_events_filter(self):
        self.logger.info({'message1': 'info', 'message2': 'message', 'event': 'FRONTIER_START'})
        log = self.logger_output.getvalue()
        pattern1 = '{bold_yellow}\d{{4}}-\d{{2}}-\d{{2}} \d{{2}}:\d{{2}}:\d{{2}},\d+ ' \
                   'FRONTIER_START   {logged_message}{reset}\n'.format(bold_yellow=re.escape(colors['bold_yellow']),
                                                                       logged_message='message info',
                                                                       reset=re.escape(colors['reset']))
        pattern2 = '{bold_yellow}\d{{4}}-\d{{2}}-\d{{2}} \d{{2}}:\d{{2}}:\d{{2}},\d+ ' \
                   'FRONTIER_START   {logged_message}{reset}\n'.format(bold_yellow=re.escape(colors['bold_yellow']),
                                                                       logged_message='info message',
                                                                       reset=re.escape(colors['reset']))
        assert re.match(pattern1, log) is not None or re.match(pattern2, log) is not None


class TestHandlerConsole(BaseTestHandlers):
    handler = CONSOLE

    def test_handler_color_based_on_level(self):
        self.logger.debug('debug message')
        self.logger.info('info message')
        self.logger.error('error message')
        self.assertEqual(self.logger_output.getvalue(),
                         '{white}[frontera] debug message{reset}\n'
                         '{green}[frontera] info message{reset}\n'
                         '{red}[frontera] error message{reset}\n'.format(white=colors['white'],
                                                                         green=colors['green'],
                                                                         red=colors['red'],
                                                                         reset=colors['reset']))


class TestHandlerConsoleManager(TestHandlerConsole):
    handler = CONSOLE_MANAGER


class TestHandlerConsoleBackend(TestHandlerConsole):
    handler = CONSOLE_BACKEND


class TestHandlerConsoleDebugging(TestHandlerConsole):
    handler = CONSOLE_DEBUGGING
