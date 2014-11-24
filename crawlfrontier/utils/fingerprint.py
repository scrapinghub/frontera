import hashlib


def sha1(key):
    return hashlib.sha1(key.encode('utf8')).hexdigest()


def md5(key):
    return hashlib.md5(key.encode('utf8')).hexdigest()
