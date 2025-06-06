import pytest

pytest.importorskip("colorlog")

import datetime
import json
import re
import unittest

from frontera.logger.formatters import CONSOLE
from frontera.logger.formatters.color import ColorFormatter
from frontera.logger.formatters.json import JSONFormatter
from tests.utils import LoggingCaptureMixin, SetupDefaultLoggingMixin, colors


class BaseTestFormatters(
    SetupDefaultLoggingMixin, LoggingCaptureMixin, unittest.TestCase
):
    def setUp(self):
        super().setUp()
        self.default_formatter = self.logger.handlers[0].formatter

    def tearDown(self):
        super().setUp()
        self.logger.handlers[0].formatter = self.default_formatter

    def setFormatter(self, formatter):
        self.logger.handlers[0].setFormatter(formatter)


class TestFormatterColor(BaseTestFormatters):
    def test_formatter_color(self):
        c = ColorFormatter(
            format="%(log_color)s [%(name)s] %(message)s",
            log_colors={
                "DEBUG": "white",
                "INFO": "green",
                "ERROR": "red",
            },
            log_color_field="levelname",
        )
        self.setFormatter(c)
        self.logger.debug("debug message")
        self.logger.info("info message")
        self.logger.error("error message")
        self.assertEqual(
            self.logger_output.getvalue(),
            "{white} [frontera] debug message{reset}\n"
            "{green} [frontera] info message{reset}\n"
            "{red} [frontera] error message{reset}\n".format(
                white=colors["white"],
                green=colors["green"],
                red=colors["red"],
                reset=colors["reset"],
            ),
        )

    def test_formatter_color_datefmt(self):
        c = ColorFormatter(
            format="%(log_color)s %(asctime)s [%(name)s] %(message)s",
            log_colors={
                "DEBUG": "white",
                "INFO": "green",
                "ERROR": "red",
            },
            datefmt="%d-%m-%Y %H:%M:%S",
            log_color_field="levelname",
        )
        self.setFormatter(c)
        self.logger.debug("debug message")
        self.assertRegex(
            self.logger_output.getvalue(),
            r"{white} \d{{2}}-\d{{2}}-\d{{4}} \d{{2}}:\d{{2}}:\d{{2}} "
            "\\[frontera\\] debug message{reset}\n".format(
                white=re.escape(colors["white"]), reset=re.escape(colors["reset"])
            ),
        )


class TestFormatterJson(BaseTestFormatters):
    def setUp(self):
        super().setUp()
        self.setFormatter(JSONFormatter())

    def test_formatter_json_log_text(self):
        self.logger.debug("debug message")
        self.assertEqual(
            json.loads(self.logger_output.getvalue())["message"], "debug message"
        )

    def test_formatter_json_log_dict(self):
        dct_msg = {
            "message": "debug message",
            "extra": "value",
        }
        self.logger.debug(dct_msg)
        json_log = json.loads(self.logger_output.getvalue())
        self.assertEqual(json_log.get("message"), "debug message")
        self.assertEqual(json_log.get("extra"), "value")

    def test_formatter_json_log_datetime_objects(self):
        dct_msg = {
            "message": "debug message",
            "datetime": datetime.datetime(2016, 9, 19, 23, 59),
            "date": datetime.date(2016, 9, 20),
            "timedelta": datetime.datetime(2016, 9, 19, 23, 59)
            - datetime.datetime(2016, 9, 19, 23, 50),
        }
        self.logger.debug(dct_msg)
        json_log = json.loads(self.logger_output.getvalue())
        self.assertEqual(json_log.get("message"), "debug message")
        self.assertEqual(json_log.get("datetime"), "2016-09-19T23:59:00")
        self.assertEqual(json_log.get("date"), "2016-09-20")
        self.assertEqual(json_log.get("timedelta"), "00:09:00")


class TestFormatterMiscellaneous(BaseTestFormatters):
    def test_formatter_console(self):
        self.setFormatter(CONSOLE)
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
