from __future__ import absolute_import
from collections import OrderedDict
import json


from frontera.utils.encoders import DateTimeEncoder


class OrderedAttrDict(OrderedDict):
    def __setattr__(self, key, value):
        if key.startswith('_'):
            super(OrderedAttrDict, self).__setattr__(key, value)
        else:
            return super(OrderedAttrDict, self).__setitem__(key, value)

    def __getattr__(self, key):
        if key.startswith('_'):
            return super(OrderedAttrDict, self).__getattr__(key)
        else:
            return self[key]

    def __repr__(self, _repr_running={}):
        return '<{0}:{1}>{2}'.format(
            self.__class__.__name__,
            hex(id(self)),
            json.dumps(self, indent=4, cls=DateTimeEncoder, sort_keys=True),
        )
