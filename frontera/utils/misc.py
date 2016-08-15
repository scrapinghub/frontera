from __future__ import absolute_import
from importlib import import_module
from zlib import crc32
from six.moves import range
from w3lib.util import to_bytes
import six


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
    """ signed crc32 of bytes or unicode.
    In python 3, return the same number as in python 2, converting to
    [-2**31, 2**31-1] range. This is done to maintain backwards compatibility
    with python 2, since checksums are stored in the database, so this allows
    to keep the same database schema.
    """
    return to_signed32(crc32(to_bytes(name, 'utf-8', 'ignore')))


def to_signed32(x):
    """ If x is an usigned 32-bit int, convert it to a signed 32-bit.
    """
    return x - 0x100000000 if x > 0x7fffffff else x


def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i+n]


def dict_to_bytes(obj):
    if isinstance(obj, dict):
        return {dict_to_bytes(k): dict_to_bytes(v) for k, v in six.iteritems(obj)}
    if isinstance(obj, six.text_type):
        return obj.encode('utf8')
    if isinstance(obj, list):
        return map(dict_to_bytes, obj)
    else:
        return obj


def dict_to_unicode(obj):
    if isinstance(obj, dict):
        return {dict_to_unicode(k): dict_to_unicode(v) for k, v in six.iteritems(obj)}
    if isinstance(obj, six.binary_type):
        return obj.decode('utf8')
    if isinstance(obj, list):
        return map(dict_to_unicode, obj)
    else:
        return obj