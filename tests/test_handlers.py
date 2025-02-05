import pytest

pytest.importorskip("colorlog")

import logging
import logging.config
import unittest

from frontera.logger.handlers import CONSOLE
from tests.utils import LoggingCaptureMixin, SetupDefaultLoggingMixin, colors


class SetupHandler(SetupDefaultLoggingMixin):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        logger = logging.getLogger("frontera")
        logger.handlers[0] = cls.handler


class TestHandlerConsole(SetupHandler, LoggingCaptureMixin, unittest.TestCase):
    handler = CONSOLE

    def test_handler_color_based_on_level(self):
        self.logger.debug("debug message")
        self.logger.info("info message")
        self.logger.error("error message")
        self.assertEqual(
            self.logger_output.getvalue(),
            "{white}[frontera] debug message{reset}\n"
            "{green}[frontera] info message{reset}\n"
            "{red}[frontera] error message{reset}\n".format(
                white=colors["white"],
                green=colors["green"],
                red=colors["red"],
                reset=colors["reset"],
            ),
        )
