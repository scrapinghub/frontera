import six


def restruct_for_pack(obj):
    """Recursively walk object's hierarchy."""
    if isinstance(obj, six.text_type):
        return obj
    if isinstance(obj, (bool, six.integer_types, float, six.binary_type)):
        return obj
    elif isinstance(obj, dict):
        obj = obj.copy()
        for key in obj:
            obj[key] = restruct_for_pack(obj[key])
        return obj
    elif isinstance(obj, list) or isinstance(obj, set):
        return [restruct_for_pack(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(restruct_for_pack([item for item in obj]))
    elif hasattr(obj, '__dict__'):
        return restruct_for_pack(obj.__dict__)
    else:
        return None