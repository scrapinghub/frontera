from importlib import import_module
from zlib import crc32

from w3lib.util import to_bytes


def load_object(path):
    """Load an object given its absolute object path, and return it.

    object can be a class, function, variable o instance.
    path ie: 'myproject.frontier.models.Page'
    """

    try:
        dot = path.rindex(".")
    except ValueError as e:
        raise ValueError(f"Error loading object '{path}': not a full path") from e

    module, name = path[:dot], path[dot + 1 :]
    try:
        mod = import_module(module)
    except ImportError as e:
        raise ImportError(f"Error loading object '{path}': {e}") from e

    try:
        obj = getattr(mod, name)
    except AttributeError as e:
        raise NameError(
            f"Module '{module}' doesn't define any object named '{name}'"
        ) from e

    return obj


def get_crc32(name):
    """signed crc32 of bytes or unicode.
    In python 3, return the same number as in python 2, converting to
    [-2**31, 2**31-1] range. This is done to maintain backwards compatibility
    with python 2, since checksums are stored in the database, so this allows
    to keep the same database schema.
    """
    return to_signed32(crc32(to_bytes(name, "utf-8", "ignore")))


def to_signed32(x):
    """If x is an usigned 32-bit int, convert it to a signed 32-bit."""
    return x - 0x100000000 if x > 0x7FFFFFFF else x


def chunks(l, n):  # noqa: E741
    for i in range(0, len(l), n):
        yield l[i : i + n]


def dict_to_bytes(obj):
    if isinstance(obj, dict):
        return {dict_to_bytes(k): dict_to_bytes(v) for k, v in obj.items()}
    if isinstance(obj, str):
        return obj.encode("utf8")
    if isinstance(obj, list):
        return map(dict_to_bytes, obj)
    return obj


def dict_to_unicode(obj):
    if isinstance(obj, dict):
        return {dict_to_unicode(k): dict_to_unicode(v) for k, v in obj.items()}
    if isinstance(obj, bytes):
        return obj.decode("utf8")
    if isinstance(obj, list):
        return map(dict_to_unicode, obj)
    return obj
