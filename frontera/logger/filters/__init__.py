from __future__ import absolute_import
import logging
import six
from w3lib.util import to_native_str


class PlainValuesFilter(logging.Filter):
    def __init__(self, separator=None, excluded_fields=None, msg_max_length=0):
        super(PlainValuesFilter, self).__init__()
        self.separator = to_native_str(separator or " ")
        self.excluded_fields = excluded_fields or []
        self.msg_max_length = msg_max_length

    def filter(self, record):
        if isinstance(record.msg, dict):
            for field_name in self.excluded_fields:
                setattr(record, field_name, record.msg.get(field_name, ''))
            record.msg = self.separator.join([to_native_str(value)
                                              for key, value in six.iteritems(record.msg)
                                              if key not in self.excluded_fields])
            if self.msg_max_length and len(record.msg) > self.msg_max_length:
                record.msg = record.msg[0:self.msg_max_length-3] + "..."

        return True


class FilterFields(logging.Filter):
    def __init__(self, field_name):
        super(FilterFields, self).__init__()
        self.field_name = field_name

    def _get_field(self, record):
        if not self.field_name:
            return None
        elif hasattr(record, self.field_name):
            return getattr(record, self.field_name)
        elif isinstance(record.msg, dict) and self.field_name in record.msg:
            return record.msg[self.field_name]
        else:
            return None


class IncludeFields(FilterFields):
    def __init__(self, field_name, included_values):
        super(IncludeFields, self).__init__(field_name)
        self.included_values = included_values

    def filter(self, record):
        field = self._get_field(record)
        if field:
            return field in self.included_values
        return True


class ExcludeFields(FilterFields):
    def __init__(self, field_name, excluded_fields):
        super(ExcludeFields, self).__init__(field_name)
        self.excluded_fields = excluded_fields

    def filter(self, record):
        field = self._get_field(record)
        if field:
            return field not in self.excluded_fields
        return True


PLAINVALUES = PlainValuesFilter
INCLUDEFIELDS = IncludeFields
EXCLUDEFIELDS = ExcludeFields
