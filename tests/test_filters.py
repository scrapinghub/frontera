import unittest

from frontera.logger.filters import EXCLUDEFIELDS, INCLUDEFIELDS, PLAINVALUES
from tests.utils import LoggingCaptureMixin, SetupDefaultLoggingMixin


class BaseTestFilters(SetupDefaultLoggingMixin, LoggingCaptureMixin, unittest.TestCase):
    def tearDown(self):
        super().setUp()
        self.logger.handlers[0].filters = []

    def addFilter(self, filter):
        self.logger.handlers[0].addFilter(filter)


class TestFilterPlainValues(BaseTestFilters):
    def test_plain_values_exclude_fields(self):
        filter = PLAINVALUES(excluded_fields=["event"])
        self.addFilter(filter)
        self.logger.debug(
            {"message1": "logging", "message2": "debug", "event": "value"}
        )
        log_msg = self.logger_output.getvalue()
        assert log_msg in ("logging debug\n", "debug logging\n")

    def test_plain_values_separator(self):
        filter = PLAINVALUES(separator=",")
        self.addFilter(filter)
        self.logger.debug({"message1": "logging", "message2": "debug"})
        log_msg = self.logger_output.getvalue()
        assert log_msg in ("logging,debug\n", "debug,logging\n")

    def test_plain_values_msg_max_length(self):
        filter = PLAINVALUES(msg_max_length=10)
        self.addFilter(filter)
        self.logger.debug({"message1": "1" * 10, "message2": "2" * 10})
        log_msg = self.logger_output.getvalue()
        assert log_msg in (f"{'1' * 7}...\n", f"{'2' * 7}...\n")

    def test_plain_values_str_msg(self):
        filter = PLAINVALUES(msg_max_length=10)
        self.addFilter(filter)
        self.logger.debug("debug message")
        self.assertEqual(self.logger_output.getvalue(), "debug message\n")


class TestIncludeFields(BaseTestFilters):
    def test_include_fields_matching_values(self):
        filter = INCLUDEFIELDS(field_name="event", included_values=["page_crawled"])
        self.addFilter(filter)
        self.logger.debug("crawled page P", extra={"event": "page_crawled"})
        self.assertEqual(self.logger_output.getvalue(), "crawled page P\n")

    def test_include_fields_non_matching_values(self):
        filter = INCLUDEFIELDS(field_name="event", included_values=["links_extracted"])
        self.addFilter(filter)
        self.logger.debug("crawled page P", extra={"event": "page_crawled"})
        self.assertEqual(self.logger_output.getvalue(), "")

    def test_include_fields_dict_msg_matching_values(self):
        filter = INCLUDEFIELDS(field_name="event", included_values=["page_crawled"])
        self.addFilter(filter)
        self.logger.debug({"message": "debug message", "event": "page_crawled"})
        log_msg = self.logger_output.getvalue()
        assert log_msg in (
            "{'event': 'page_crawled', 'message': 'debug message'}\n",
            "{'message': 'debug message', 'event': 'page_crawled'}\n",
        )

    def test_include_fields_dict_msg_non_matching_values(self):
        filter = INCLUDEFIELDS(field_name="event", included_values=["links_extracted"])
        self.addFilter(filter)
        self.logger.debug({"message": "debug message", "event": "page_crawled"})
        self.assertEqual(self.logger_output.getvalue(), "")

    def test_include_fields_field_name_none(self):
        filter = INCLUDEFIELDS(field_name=None, included_values=[])
        self.addFilter(filter)
        self.logger.debug("debug message")
        self.assertEqual(self.logger_output.getvalue(), "debug message\n")

    def test_include_fields_list_message(self):
        filter = INCLUDEFIELDS(field_name="event", included_values=["page_crawled"])
        self.addFilter(filter)
        self.logger.debug(["debug message"])
        self.assertEqual(self.logger_output.getvalue(), "['debug message']\n")


class TestExcludeFields(BaseTestFilters):
    def test_exclude_fields_matching_values(self):
        filter = EXCLUDEFIELDS(field_name="event", excluded_fields=["page_crawled"])
        self.addFilter(filter)
        self.logger.debug("crawled page P", extra={"event": "page_crawled"})
        self.assertEqual(self.logger_output.getvalue(), "")

    def test_exclude_fields_non_matching_values(self):
        filter = EXCLUDEFIELDS(field_name="event", excluded_fields=["links_extracted"])
        self.addFilter(filter)
        self.logger.debug("crawled page P", extra={"event": "page_crawled"})
        self.assertEqual(self.logger_output.getvalue(), "crawled page P\n")

    def test_exclude_fields_dict_msg_matching_values(self):
        filter = EXCLUDEFIELDS(field_name="event", excluded_fields="page_crawled")
        self.addFilter(filter)
        self.logger.debug({"message": "debug message", "event": "page_crawled"})
        self.assertEqual(self.logger_output.getvalue(), "")

    def test_exclude_fields_dict_msg_non_matching_values(self):
        filter = EXCLUDEFIELDS(field_name="event", excluded_fields="links_extracted")
        self.addFilter(filter)
        self.logger.debug({"message": "debug message", "event": "page_crawled"})
        log_msg = self.logger_output.getvalue()
        assert log_msg in (
            "{'event': 'page_crawled', 'message': 'debug message'}\n",
            "{'message': 'debug message', 'event': 'page_crawled'}\n",
        )

    def test_include_fields_field_name_none(self):
        filter = EXCLUDEFIELDS(field_name=None, excluded_fields=[])
        self.addFilter(filter)
        self.logger.debug("debug message")
        self.assertEqual(self.logger_output.getvalue(), "debug message\n")

    def test_include_fields_list_message(self):
        filter = EXCLUDEFIELDS(field_name="event", excluded_fields=["page_crawled"])
        self.addFilter(filter)
        self.logger.debug(["debug message"])
        self.assertEqual(self.logger_output.getvalue(), "['debug message']\n")
