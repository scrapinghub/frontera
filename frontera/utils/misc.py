from importlib import import_module
from zlib import crc32


def load_object(path):
    """Load an object given its absolute object path, and return it.

    object can be a class, function, variable o instance.
    path ie: 'myproject.frontier.models.Page'
    """

    try:
        dot = path.rindex('.')
    except ValueError:
        raise ValueError("Error loading object '%s': not a full path" % path)

    module, name = path[:dot], path[dot+1:]
    try:
        mod = import_module(module)
    except ImportError as e:
        raise ImportError("Error loading object '%s': %s" % (path, e))

    try:
        obj = getattr(mod, name)
    except AttributeError:
        raise NameError("Module '%s' doesn't define any object named '%s'" % (module, name))

    return obj


def get_crc32(name):
    return crc32(name) if type(name) is str else crc32(name.encode('utf-8', 'ignore'))


def chunks(l, n):
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


