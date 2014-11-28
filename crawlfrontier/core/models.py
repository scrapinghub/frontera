from crawlfrontier.utils.collections import OrderedAttrDict
from crawlfrontier.exceptions import MissingRequiredField


class Field(object):
    def __init__(self, default=None, required=False, order=0):
        self.default = default
        self.required = required
        self.order = order


class Meta(object):
    def __init__(self):
        self.fields = dict()

    def merge(self, new_options):
        self.fields.update(new_options.fields)


class ModelMetaClass(type):
    def __new__(mcs, name, bases, attrs):
        new_attrs = {}
        if name != 'Model':
            meta = attrs.get('_meta', Meta())
            for base in bases:
                if hasattr(base, '_meta'):
                    meta.merge(base._meta)
            for attr_name, attr_value in attrs.iteritems():
                if isinstance(attr_value, Field):
                    meta.fields[attr_name] = attr_value
                else:
                    new_attrs[attr_name] = attr_value
            new_attrs['_meta'] = meta
        else:
            new_attrs = attrs
        return super(ModelMetaClass, mcs).__new__(mcs, name, bases, new_attrs)


class Model(OrderedAttrDict):
    __metaclass__ = ModelMetaClass

    def __init__(self, *args, **kwargs):
        super(Model, self).__init__(*args, **kwargs)
        for field_name, field in self._meta.fields.items():
            if field_name not in self:
                self[field_name] = field.default
            if field.required and self[field_name] is None:
                raise MissingRequiredField('Missing required field "%s"' % field_name)

        sorted_fields = sorted(self._meta.fields.items(), key=lambda x: x[1].order)
        sorted_values = [(key, self[key]) for key, _ in sorted_fields]
        self.clear()
        for name, value in sorted_values:
            self[name] = value

        self.init_fields()

    def init_fields(self):
        pass


class Link(Model):
    url = Field(required=True)


class Page(Model):
    url = Field(required=True)


class BasicLink(Link):
    pass


class BasicPage(Page):
    class State(object):
        NOT_CRAWLED = 'N'
        QUEUED = 'Q'
        CRAWLED = 'C'
        ERROR = 'E'

    fingerprint = Field()
    state = Field(default=State.NOT_CRAWLED)
    depth = Field(default=0)
    created_at = Field()
    last_update = Field()
    status = Field()
    n_adds = Field(default=0)
    n_queued = Field(default=0)
    n_crawls = Field(default=0)
    n_errors = Field(default=0)
    meta = Field(default={})

    @property
    def is_seed(self):
        return self.depth == 0
