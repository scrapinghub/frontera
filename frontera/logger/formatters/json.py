from __future__ import absolute_import

from pythonjsonlogger.jsonlogger import JsonFormatter
from frontera.utils.encoders import DateTimeEncoder


class JSONFormatter(JsonFormatter):
    def __init__(self):
        json_encoder = DateTimeEncoder
        super(JSONFormatter, self).__init__(json_encoder=json_encoder)
