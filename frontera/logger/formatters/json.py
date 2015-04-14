from __future__ import absolute_import

from pythonjsonlogger.jsonlogger import JsonFormatter

import datetime
from json import JSONEncoder


class DateTimeEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, datetime.date):
            return obj.isoformat()
        elif isinstance(obj, datetime.timedelta):
            return (datetime.datetime.min + obj).time().isoformat()
        else:
            return super(DateTimeEncoder, self).default(obj)


class JSONFormatter(JsonFormatter):
    def __init__(self):
        json_encoder = DateTimeEncoder
        super(JSONFormatter, self).__init__(json_encoder=json_encoder)
