import six
from importlib import import_module

import default_settings


class Settings(object):
    """
    An object that holds frontier settings values.
    """
    def __init__(self, module=None, attributes=None):
        """
        :param object/string module: A :class:`Settings <crawlfrontier.settings.Settings>` object or a path string.
        :param dict attributes: A dict object containing the settings values.

        """
        self.attributes = {}
        self.add_module(default_settings)
        if module:
            self.add_module(module)
        if attributes:
            self.set_from_dict(attributes)

    @classmethod
    def from_params(cls, **kwargs):
        return cls(attributes=kwargs)

    def __getattr__(self, name):
        if name.isupper() and name in self.attributes:
            return self.attributes[name]
        else:
            return self.__dict__[name]

    def __setattr__(self, name, value):
        if name.isupper():
            self.attributes[name] = value
        else:
            self.__dict__[name] = value

    def add_module(self, module):
        if isinstance(module, Settings):
            for name, value in module.attributes.items():
                self.set(name, value)
        else:
            if isinstance(module, six.string_types):
                module = import_module(module)
            for key in dir(module):
                if key.isupper():
                    self.set(key, getattr(module, key))

    def get(self, key, default_value=None):
        if not key.isupper():
            return None
        return self.attributes.get(key, default_value)

    def set(self, key, value):
        if key.isupper():
            self.attributes[key] = value

    def set_from_dict(self, attributes):
        for name, value in attributes.items():
            self.set(name, value)
