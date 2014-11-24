import json
import copy
from collections import OrderedDict

from crawlfrontier.utils.encoders import DateTimeEncoder


class Model(object):
    def _get_values(self):
        values_dict = {}
        for attr_name in self.__dict__:
            value = getattr(self, attr_name)
            value = value._get_values() if isinstance(value, Model) else value
            values_dict[attr_name] = value
        values_dict['_'] = str(self)
        return values_dict

    def __repr__(self):
        return json.dumps(self._get_values(), indent=4, cls=DateTimeEncoder, sort_keys=True)

    def __str__(self):
        return '<%s:%s:%s>' % (self.__class__.__name__, hex(id(self)), self._name)

    @property
    def _name(self):
        raise NotImplementedError


class Link(Model):

    def __init__(self, url):
        self.url = url

    @property
    def _name(self):
        return self.url


class Page(Link):

    class State(object):
        NOT_CRAWLED = 'N'
        QUEUED = 'Q'
        CRAWLED = 'C'
        ERROR = 'E'

    def __init__(self, url):
        super(Page, self).__init__(url)
        self.url = url
        self.state = self.State.NOT_CRAWLED
        self.depth = 0
        self.created_at = None
        self.last_update = None
        self.status = None
        self.n_adds = 0
        self.n_queued = 0
        self.n_crawls = 0
        self.n_errors = 0
        self.meta = OrderedDict()

    @classmethod
    def from_link(cls, link):
        page = Page(link.url)
        for attr_name in link.__dict__:
            if not attr_name.startswith('_'):
                setattr(page, attr_name, copy.deepcopy(getattr(link, attr_name)))
        return page

    @property
    def is_seed(self):
        return self.depth == 0
